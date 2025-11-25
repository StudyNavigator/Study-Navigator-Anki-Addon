import json
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple
import base64

from aqt import mw


# Ensure vendor path so bundled requests can be used
_VENDOR_PATH = os.path.join(os.path.dirname(__file__), "vendor")
if _VENDOR_PATH not in sys.path:
    sys.path.append(_VENDOR_PATH)

try:
    import requests  # type: ignore
except Exception:  # Fallback to error later when actually used
    requests = None  # type: ignore

_shared_requests_session = None

PROXY_API_URL = "https://f6w01xou1c.execute-api.us-east-2.amazonaws.com"

# Secure storage constants
KEYCHAIN_SERVICE_NAME = "anki_study_navigator"
KEYCHAIN_USERNAME = "session_data"


def _get_addon_dir() -> str:
    addon_dirname = os.path.basename(os.path.dirname(__file__))
    addon_dir = os.path.join(mw.pm.addonFolder(), addon_dirname)
    os.makedirs(addon_dir, exist_ok=True)
    return addon_dir


def _session_file_path() -> str:
    override = os.environ.get("MY_LOGIN_ADDON_SESSION_PATH")
    if override:
        override_dir = os.path.dirname(override)
        if override_dir and not os.path.exists(override_dir):
            try:
                os.makedirs(override_dir, exist_ok=True)
            except Exception as e:
                print(f"[WARN] Could not create override dir {override_dir}: {e}")
        return override
    return os.path.join(_get_addon_dir(), "logged_in.json")


def _try_import_keyring():
    """Try to import keyring library"""
    try:
        import keyring
        return keyring
    except ImportError:
        return None


def _try_import_cryptography():
    """Try to import cryptography library"""
    try:
        from cryptography.fernet import Fernet
        return Fernet
    except ImportError:
        return None


def _get_encryption_key() -> Optional[bytes]:
    """Get or create encryption key for fallback encrypted storage"""
    Fernet = _try_import_cryptography()
    if not Fernet:
        return None
    
    key_file = os.path.join(_get_addon_dir(), ".session_key")
    
    try:
        if os.path.exists(key_file):
            with open(key_file, 'rb') as f:
                return f.read()
        else:
            # Generate new key
            key = Fernet.generate_key()
            with open(key_file, 'wb') as f:
                f.write(key)
            # Make file read-only
            try:
                os.chmod(key_file, 0o600)
            except:
                pass  # Windows doesn't support this
            return key
    except Exception as e:
        print(f"[WARN] Failed to manage encryption key: {e}")
        return None


def _save_to_keychain(session: Dict[str, Any]) -> bool:
    """Try to save session to OS keychain"""
    keyring = _try_import_keyring()
    if not keyring:
        return False
    
    try:
        session_json = json.dumps(session)
        keyring.set_password(KEYCHAIN_SERVICE_NAME, KEYCHAIN_USERNAME, session_json)
        print("[INFO] Session saved to OS keychain")
        return True
    except Exception as e:
        print(f"[WARN] Keychain save failed: {e}")
        return False


def _load_from_keychain() -> Optional[Dict[str, Any]]:
    """Try to load session from OS keychain"""
    keyring = _try_import_keyring()
    if not keyring:
        return None
    
    try:
        session_json = keyring.get_password(KEYCHAIN_SERVICE_NAME, KEYCHAIN_USERNAME)
        if session_json:
            return json.loads(session_json)
    except Exception as e:
        print(f"[WARN] Keychain load failed: {e}")
    return None


def _clear_from_keychain() -> None:
    """Try to clear session from OS keychain"""
    keyring = _try_import_keyring()
    if not keyring:
        return
    
    try:
        keyring.delete_password(KEYCHAIN_SERVICE_NAME, KEYCHAIN_USERNAME)
        print("[INFO] Session cleared from OS keychain")
    except Exception:
        pass  # May not exist


def _save_encrypted_file(session: Dict[str, Any]) -> bool:
    """Save session to encrypted file as fallback"""
    Fernet = _try_import_cryptography()
    if not Fernet:
        return False
    
    try:
        key = _get_encryption_key()
        if not key:
            return False
        
        fernet = Fernet(key)
        session_json = json.dumps(session).encode()
        encrypted = fernet.encrypt(session_json)
        
        path = _session_file_path()
        with open(path, 'wb') as f:
            f.write(encrypted)
        
        # Make file read-only
        try:
            os.chmod(path, 0o600)
        except:
            pass  # Windows doesn't support this
        
        print("[INFO] Session saved to encrypted file")
        return True
    except Exception as e:
        print(f"[WARN] Encrypted file save failed: {e}")
        return False


def _load_encrypted_file() -> Optional[Dict[str, Any]]:
    """Load session from encrypted file"""
    Fernet = _try_import_cryptography()
    if not Fernet:
        return None
    
    try:
        path = _session_file_path()
        if not os.path.exists(path):
            return None
        
        key = _get_encryption_key()
        if not key:
            return None
        
        fernet = Fernet(key)
        
        with open(path, 'rb') as f:
            encrypted = f.read()
        
        decrypted = fernet.decrypt(encrypted)
        return json.loads(decrypted)
    except Exception as e:
        print(f"[WARN] Encrypted file load failed: {e}")
        return None


def _load_plaintext_file() -> Optional[Dict[str, Any]]:
    """Load session from old plaintext file (for migration)"""
    try:
        path = _session_file_path()
        if os.path.exists(path):
            with open(path, "r") as f:
                data = json.load(f)
                print("[INFO] Migrating from plaintext session storage")
                return data
    except Exception:
        pass  # Not a plaintext file or doesn't exist
    return None


def load_session() -> Optional[Dict[str, Any]]:
    """
    Load session using secure storage (OS keychain or encrypted file).
    Falls back to plaintext for migration.
    """
    # Try OS keychain first (most secure)
    session = _load_from_keychain()
    if session:
        return session
    
    # Try encrypted file
    session = _load_encrypted_file()
    if session:
        return session
    
    # Try plaintext file (for migration from old version)
    session = _load_plaintext_file()
    if session:
        # Migrate to secure storage
        print("[INFO] Migrating session to secure storage")
        save_session(session)
        return session
    
    return None


def save_session(session: Dict[str, Any]) -> None:
    """
    Save session using secure storage.
    Tries OS keychain first, falls back to encrypted file, then plaintext as last resort.
    """
    # Try OS keychain first (most secure)
    if _save_to_keychain(session):
        return
    
    # Try encrypted file
    if _save_encrypted_file(session):
        return
    
    # Last resort: plaintext (should only happen if no crypto libraries available)
    print("[WARN] Saving session in plaintext - consider installing cryptography library")
    try:
        path = _session_file_path()
        with open(path, "w") as f:
            json.dump(session, f)
    except Exception as e:
        print(f"[ERROR] Failed to save session: {e}")


def clear_session() -> None:
    """Clear session from all storage locations"""
    # Clear from keychain
    _clear_from_keychain()
    
    # Clear file (encrypted or plaintext)
    try:
        path = _session_file_path()
        if os.path.exists(path):
            os.remove(path)
        # Also remove encryption key
        key_file = os.path.join(_get_addon_dir(), ".session_key")
        if os.path.exists(key_file):
            os.remove(key_file)
    except Exception as e:
        print(f"[ERROR] Failed to clear session: {e}")


def mark_user_synced() -> None:
    session = load_session()
    if not session:
        return
    if session.get("synced") is True:
        return
    session["synced"] = True
    save_session(session)


def is_logged_in() -> bool:
    session = load_session()
    return bool(session and session.get("logged_in", False))


def has_already_synced_user() -> bool:
    session = load_session()
    if not session:
        return False
    return bool(session.get("synced", False))


def set_session_after_login(email: str, session_payload: Dict[str, Any], synced: bool) -> None:
    # session_payload is the response from /login containing tokens and expires_at
    to_save = {
        "logged_in": True,
        "email": email,
        "access_token": session_payload.get("access_token"),
        "refresh_token": session_payload.get("refresh_token"),
        # Prefer server-provided expires_at; fallback to expires_in
        "expires_at": session_payload.get("expires_at")
            if session_payload.get("expires_at") is not None
            else int(time.time()) + int(session_payload.get("expires_in", 0)),
        "synced": synced,
    }
    save_session(to_save)


def expires_in_seconds() -> Optional[int]:
    session = load_session()
    if not session:
        return None
    expires_at = int(session.get("expires_at", 0))
    return int(expires_at - time.time())


def get_expires_at() -> Optional[int]:
    session = load_session()
    if not session:
        return None
    try:
        return int(session.get("expires_at", 0))
    except Exception:
        return None


def set_session_expiry_in(seconds_from_now: int) -> None:
    session = load_session()
    if not session:
        return
    session["expires_at"] = int(time.time()) + int(seconds_from_now)
    save_session(session)


def get_user_auth_info() -> Tuple[Optional[str], Optional[str], bool]:
    session = load_session()
    if not session:
        return None, None, False
    email = session.get("email")
    token = session.get("access_token")
    synced = bool(session.get("synced", False))
    return email, token, synced


def _ensure_requests_available() -> bool:
    global requests
    if requests is None:
        try:
            import requests as _req  # type: ignore
            requests = _req  # type: ignore
        except Exception as e:
            print(f"[ERROR] requests not available: {e}")
            return False
    return True


def get_http_session():
    global _shared_requests_session
    if not _ensure_requests_available():
        return None
    if _shared_requests_session is None:
        try:
            _shared_requests_session = requests.Session()
        except Exception as e:
            print(f"[WARN] Failed to create requests.Session(): {e}")
            return None
    return _shared_requests_session


def get_valid_token(refresh_if_within_seconds: int = 600) -> Optional[str]:
    session = load_session()
    if not session:
        return None

    access_token = session.get("access_token")
    refresh_token = session.get("refresh_token")
    expires_at = int(session.get("expires_at", 0))
    seconds_left = int(expires_at - time.time())

    if not access_token or not refresh_token:
        return None

    # Token still valid, no refresh needed
    if seconds_left > refresh_if_within_seconds:
        print(f"[INFO] Using existing token (expires in {seconds_left}s)")
        return access_token

    print(f"[INFO] Token expires soon ({seconds_left}s), attempting refresh...")

    if not _ensure_requests_available():
        # Network not available, fail gracefully
        print("[WARN] Network unavailable, using existing token")
        return access_token if seconds_left > 0 else None

    network_error = False
    status_code = None
    
    try:
        sess = get_http_session()
        http = sess if sess is not None else requests
        # Try form-encoded first (OAuth2 standard)
        resp = http.post(
            f"{PROXY_API_URL}/refresh-token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token
            },
            timeout=20,
        )
        status_code = resp.status_code
        data = {}
        try:
            data = resp.json()
        except Exception:
            pass
        
        if resp.status_code >= 400 or "access_token" not in data:
            # Retry as JSON
            print("[INFO] Retrying token refresh as JSON...")
            resp = http.post(
                f"{PROXY_API_URL}/refresh-token",
                json={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token
                },
                timeout=20,
            )
            status_code = resp.status_code
            try:
                data = resp.json()
            except Exception:
                data = {}
            
            if resp.status_code >= 400:
                print(f"[ERROR] Refresh failed: status={resp.status_code}, body={resp.text}")
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        print(f"[ERROR] Token refresh network error: {e}")
        network_error = True
        data = {}
    except Exception as e:
        print(f"[ERROR] Token refresh exception: {e}")
        network_error = True
        data = {}

    # Successfully refreshed
    if "access_token" in data:
        new_access = data["access_token"]
        new_refresh = data.get("refresh_token", refresh_token)
        # Supabase returns expires_in on refresh (usually 3600 seconds = 1 hour)
        new_expires_at = int(time.time()) + int(data.get("expires_in", 3600))

        session.update(
            {
                "access_token": new_access,
                "refresh_token": new_refresh,
                "expires_at": new_expires_at,
                "logged_in": True,
            }
        )
        save_session(session)
        print(f"[INFO] Token refreshed successfully (expires in {data.get('expires_in', 3600)}s)")
        return new_access

    # Determine if we should clear the session or keep retrying
    should_clear_session = False
    
    if status_code == 401 or status_code == 403:
        print(f"[ERROR] Refresh token is invalid or expired (status={status_code})")
        should_clear_session = True
    elif status_code == 400:
        try:
            error_response = resp.json() if 'resp' in locals() else {}
            error_code = error_response.get("error_code", "")
            error = error_response.get("error", "")
            
            # Permanent errors that require re-authentication
            permanent_errors = [
                "invalid_grant",
                "invalid_token", 
                "token_expired",
                "token_revoked"
            ]
            
            if error_code in permanent_errors or error in permanent_errors:
                print(f"[ERROR] Permanent refresh failure: {error_code or error}")
                should_clear_session = True
            else:
                print(f"[WARN] Temporary refresh failure: status={status_code}, network_error={network_error}")
        except Exception:
            # Can't determine error type, treat as temporary
            print(f"[WARN] Temporary refresh failure: status={status_code}, network_error={network_error}")
    elif status_code and status_code >= 500:
        # 5xx Server Error = temporary server issue, keep session
        print(f"[WARN] Server error during refresh (status={status_code}), will retry later")
    elif network_error:
        # Network errors are temporary
        print("[WARN] Network error during refresh, will retry later")
    else:
        # Unknown error, treat as temporary
        print(f"[WARN] Unknown refresh error (status={status_code}), will retry later")
    
    if should_clear_session:
        print("[INFO] Clearing session - user will need to log in again")
        clear_session()
        return None
    
    # Temporary failure: keep the session and continue gracefully
    if seconds_left > 0:
        print(f"[INFO] Token refresh failed temporarily, using existing token ({seconds_left}s remaining)")
        return access_token
    else:
        print("[INFO] Token expired but refresh failed temporarily - will retry automatically")
        print("[INFO] Allowing operations with expired token (temporary grace period)")
        save_session(session)
        return access_token


def warm_proxy_auth_endpoints() -> None:
    # Best-effort warmup to reduce cold start latency during login
    if not _ensure_requests_available():
        return
    try:
        sess = get_http_session()
        http = sess if sess is not None else requests
        http.get(f"{PROXY_API_URL}/credentials", timeout=5)
    except Exception:
        pass
    try:
        sess = get_http_session()
        http = sess if sess is not None else requests
        http.options(f"{PROXY_API_URL}/login", timeout=5)
    except Exception:
        pass
