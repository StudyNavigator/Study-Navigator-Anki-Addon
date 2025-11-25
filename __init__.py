from aqt import mw, gui_hooks
from aqt.qt import QAction, QMenu
from aqt.utils import showInfo
from PyQt6.QtWidgets import QMessageBox
from PyQt6.QtCore import QTimer

from .login_dialog import LoginDialog
from .session_store import is_logged_in, clear_session, get_valid_token, warm_proxy_auth_endpoints

from .optimized_tag_exporter import add_unified_export_menu_action

# Global state
login_action = None
_refresh_timer = None
study_navigator_menu = None


# Login UI
def login_ui_action():
    global login_action

    if is_logged_in():
        response = QMessageBox.question(
            mw,
            "Already Logged In",
            "You're currently logged in. Would you like to log out of Study Navigator?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if response == QMessageBox.StandardButton.Yes:
            clear_session()
            showInfo("You have been logged out.")
            login_action.setText("Log in to Study Navigator")
        else:
            showInfo("You're still logged in.")
    else:
        dlg = LoginDialog(mw)
        dlg.exec()

        if is_logged_in():
            login_action.setText("Log out of Study Navigator")


# Menu Setup
def add_menu_items():
    global login_action, study_navigator_menu
    
    # Create the Study Navigator menu at the end of the menu bar
    study_navigator_menu = QMenu("Study Navigator", mw)
    mw.form.menubar.addMenu(study_navigator_menu)

    # Login/Logout action
    login_action = QAction(
        "Log out of Study Navigator" if is_logged_in() else "Log in to Study Navigator",
        mw
    )
    login_action.triggered.connect(login_ui_action)
    study_navigator_menu.addAction(login_action)
    
    # Add separator
    study_navigator_menu.addSeparator()

    # Sync to Study Navigator
    add_unified_export_menu_action(study_navigator_menu)

# Hook to add menu items and start background token refresh when main window is ready
def _start_token_refresh_timer():
    global _refresh_timer
    if _refresh_timer is not None:
        return
    _refresh_timer = QTimer(mw)
    _refresh_timer.setInterval(10 * 60 * 1000)  # 10 minutes
    def refresh_in_background():
        try:
            token = get_valid_token(refresh_if_within_seconds=600)
            if token:
                pass  # Success, token is valid
            else:
                # Token couldn't be refreshed, but session may still be kept
                # for temporary failures. Next timer cycle will retry.
                pass
        except Exception as e:
            # Silently handle exceptions in background refresh
            print(f"[WARN] Background token refresh exception: {e}")
    
    _refresh_timer.timeout.connect(refresh_in_background)
    _refresh_timer.start()


def on_main_window_did_init():
    add_menu_items()
    # Immediate check/refresh on startup (silent, no user prompts)
    try:
        get_valid_token(refresh_if_within_seconds=600)
    except Exception:
        # Silently fail - user will only be prompted if they try to use a feature
        pass
    # Warm auth endpoints early to reduce login latency
    try:
        warm_proxy_auth_endpoints()
    except Exception:
        pass
    _start_token_refresh_timer()


gui_hooks.main_window_did_init.append(on_main_window_did_init)
