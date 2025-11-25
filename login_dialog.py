import os
import sys
import json
import subprocess
import re
from aqt import mw
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QLabel, QLineEdit, QPushButton, QMessageBox
)
from PyQt6.QtGui import QFont
from typing import Union
from PyQt6.QtCore import Qt
from .session_store import (
    set_session_after_login,
    has_already_synced_user,
    warm_proxy_auth_endpoints,
    get_http_session,
)

# Input validation functions
def validate_email_format(email: str) -> tuple[bool, str]:
    """Validate email format"""
    if not email or len(email) > 255:
        return False, "Please enter a valid email address"
    
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_pattern, email):
        return False, "Please enter a valid email address"
    
    return True, ""


def validate_password_present(password: str) -> tuple[bool, str]:
    """Validate password is present"""
    if not password:
        return False, "Password is required"
    
    if len(password) > 128:
        return False, "Password too long (max 128 characters)"
    
    return True, ""


# Proxy API Config
PROXY_API_URL = "https://f6w01xou1c.execute-api.us-east-2.amazonaws.com"


# Supabase Login via Proxy
def supabase_login(email: str, password: str) -> tuple[bool, Union[dict, str]]:
    import requests

    try:
        # Reuse a shared session to speed up TLS and reduce latency
        sess = get_http_session()
        http = sess if sess is not None else requests

        # Use proxy API for login to avoid exposing Supabase credentials
        response = http.post(
            f"{PROXY_API_URL}/login",
            json={"email": email, "password": password},
            timeout=20,
        )

        data = response.json()
        if response.status_code == 200 and "access_token" in data:
            return True, data
        else:
            return False, data.get("error_description", "Login failed.")
    except Exception as e:
        return False, str(e)


# Supabase Insert to public.users via proxy
def upsert_public_user(email: str, user_id: str, access_token: str):
    import requests

    try:
        response = requests.post(
            f"{PROXY_API_URL}/upsert-user",
            json={
                "id": user_id,
                "email": email,
                "anki_user_id": user_id
            },
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            },
            timeout=20
        )
        if response.status_code == 200:
            print(f"[SUCCESS] ✓ User synced to database successfully!")
        else:
            print(f"[ERROR] Failed to upsert user: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[ERROR] Exception upserting user: {e}")


# Login Dialog UI
class LoginDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Log in to Study Navigator")
        self.setFixedSize(420, 280)
        self.setStyleSheet("""
            QDialog {
                background-color: #ffffff;
                border-radius: 12px;
            }
            QLabel {
                font-size: 18px;
                font-weight: normal;
                color: #000000;
                padding: 8px 0px;
            }
            QLineEdit {
                padding: 12px;
                font-size: 14px;
                border: 1px solid #e0e0e0;
                border-radius: 8px;
                background-color: #f9f9f9;
                color: #000000;
            }
            QLineEdit:focus {
                border: 1px solid #000000;
                background-color: #ffffff;
            }
            QPushButton {
                padding: 12px;
                font-size: 14px;
                background-color: #000000;
                color: #ffffff;
                border: none;
                border-radius: 8px;
            }
            QPushButton:hover {
                background-color: #222222;
            }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(14)

        self.label = QLabel("Welcome to Study Navigator")
        font = QFont("Helvetica")
        self.label.setFont(font)
        layout.addWidget(self.label)
        self.label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.email_input = QLineEdit()
        self.email_input.setPlaceholderText("Email")
        layout.addWidget(self.email_input)

        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("Password")
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        layout.addWidget(self.password_input)

        self.login_button = QPushButton("Log In")
        self.login_button.clicked.connect(self.handle_login)
        layout.addWidget(self.login_button)
        # Warm proxy endpoints to avoid cold-start during login
        try:
            warm_proxy_auth_endpoints()
        except Exception:
            pass

    def handle_login(self):
        email = self.email_input.text().strip()
        password = self.password_input.text().strip()

        if not email or not password:
            QMessageBox.warning(self, "Error", "Please enter both fields.")
            return
        
        # Validate email format
        valid_email, email_error = validate_email_format(email)
        if not valid_email:
            QMessageBox.warning(self, "Invalid Email", email_error)
            return
        
        # Validate password is present (no strength check - that's for signup on website)
        valid_password, password_error = validate_password_present(password)
        if not valid_password:
            QMessageBox.warning(self, "Invalid Password", password_error)
            return

        success, result = supabase_login(email, password)

        if success:
            access_token = result["access_token"]
            user_id = result["user"]["id"]
            email = result["user"]["email"]

            # Save session immediately to close dialog fast
            set_session_after_login(email=email, session_payload=result, synced=has_already_synced_user())
            # Offload upsert to background, then mark synced on success
            def _bg_upsert():
                try:
                    if not has_already_synced_user():
                        upsert_public_user(email, user_id, access_token)
                        from .session_store import mark_user_synced
                        mark_user_synced()
                except Exception as e:
                    print(f"[WARN] Background upsert failed: {e}")

            try:
                import threading
                threading.Thread(target=_bg_upsert, daemon=True).start()
            except Exception:
                # Fallback run inline if threads not available (rare)
                _bg_upsert()
            QMessageBox.information(self, "Success", "Logged in successfully!")
            self.accept()
        else:
            QMessageBox.critical(self, "Login Failed", result)
