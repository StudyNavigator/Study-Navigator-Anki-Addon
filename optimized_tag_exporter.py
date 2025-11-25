import os
from typing import Tuple

from aqt import mw
from aqt.qt import QAction
from aqt.utils import showInfo
from aqt.operations import QueryOp

from .session_store import get_user_auth_info


def export_all_tags_data():
    """
    Export ALL tag data from the Tags section without any filtering.
    """
    
    # Check if user is logged in BEFORE processing anything
    from .session_store import is_logged_in
    if not is_logged_in():
        showInfo("⚠️ Please log in to Study Navigator before syncing.\n\nUse: Study Navigator → Log in to Study Navigator")
        return
    
    def _run_all_tags_export_background(col):
        """This runs in background thread - NO UI CALLS HERE"""
        try:
            print("[INFO] Starting sync to Study Navigator...")
            
            # Import the unified exporter (now exports all tags)
            from .unified_card_exporter import export_all_tags_data
            
            # Run the all tags export
            results = export_all_tags_data()
            
            # Extract results
            unified = results['unified_export'] 
            timing = results['timing']
            
            # Upload the unified file to AWS
            upload_results = []
            print("[INFO] Uploading data to Study Navigator...")
            
            try:
                from .upload_AWS import upload_compressed_file
                
                # Upload unified file
                if unified.get('success', False) and unified.get('file_path'):
                    print("[INFO] Uploading data file to AWS...")
                    upload_success = upload_compressed_file(unified['file_path'], file_type='unified')
                    upload_results.append({
                        'file': 'DATA',
                        'path': unified['file_path'],
                        'success': upload_success
                    })
                    
            except ImportError:
                print("[WARNING] Upload module not available")
                upload_results = []
            except Exception as upload_error:
                print(f"[ERROR] Upload failed: {upload_error}")
                upload_results = []
            
            # Return results for success callback
            return {
                'unified': unified,
                'timing': timing,
                'upload_results': upload_results
            }
            
        except Exception as e:
            print(f"[ERROR] Sync to Study Navigator failed: {str(e)}")
            # Return error for success callback
            return {'error': str(e)}
    
    def _on_success(result):
        """This runs on main thread - UI CALLS ARE SAFE HERE"""
        try:
            if 'error' in result:
                error_msg = f"Sync to Study Navigator failed: {result['error']}"
                showInfo(error_msg)
                return
            
            # Extract results
            unified = result['unified']
            timing = result['timing']
            upload_results = result['upload_results']
            
            # Build success message
            success_msg = "✅ Sync to Study Navigator Complete!\n\n"
            success_msg += "📊 Data Synced:\n"
            
            # All tags export
            if unified.get('success', False):
                success_msg += f"• {unified['record_count']:,} tag records\n"
                success_msg += f"• {unified['unique_cards']:,} unique cards\n"
            else:
                success_msg += f"❌ Sync Failed\n"
                if 'error' in unified:
                    success_msg += f"   Error: {unified['error']}\n"
            
            # Timing
            success_msg += f"\n⏱️ Sync Time: {timing['total_time']:.2f} seconds"
            
            # Upload status
            if upload_results:
                successful_uploads = 0
                for result in upload_results:
                    if result['success']:
                        successful_uploads += 1
                
                if successful_uploads == len(upload_results):
                    success_msg += f"\n\n🎉 Your data is now syncing to Study Navigator!"
                    success_msg += f"\n💡 Refresh your browser to see updated stats"
                else:
                    success_msg += f"\n\n⚠️ Upload failed - check console for details"
            else:
                success_msg += f"\n\n⚠️ Upload module not available - file saved locally only"
            
            showInfo(success_msg)
            
        except Exception as e:
            error_msg = f"Failed to display sync results: {str(e)}"
            print(f"[ERROR] {error_msg}")
            showInfo(error_msg)
    
    # Run in background with proper thread handling
    try:
        QueryOp(
            parent=mw,
            op=_run_all_tags_export_background,
            success=_on_success
        ).run_in_background()
        print("[DEBUG] Sync to Study Navigator started successfully")
    except Exception as e:
        error_msg = f"Failed to start sync to Study Navigator: {str(e)}"
        print(f"[ERROR] {error_msg}")
        showInfo(error_msg)

def add_unified_export_menu_action(menu=None):
    """Add sync action to the specified menu and return the action"""
    if menu is None:
        menu = mw.form.menuTools
    
    unified_action = QAction("Sync to Study Navigator", mw)
    unified_action.triggered.connect(export_all_tags_data)
    menu.addAction(unified_action)
    print("[INFO] Added sync to Study Navigator menu action")
    return unified_action
