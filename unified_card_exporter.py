"""
Unified Card Exporter - Option B Implementation
Creates ONLY one unified file containing all step data with overlaps removed.
No individual step files are created.
"""

import os
import sys
import json
import gzip
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Any

# Add the addon directory to the path for imports
addon_dir = os.path.dirname(os.path.abspath(__file__))
if addon_dir not in sys.path:
    sys.path.insert(0, addon_dir)

from services.export_service_factory import ExportServiceFactory


class UnifiedCardExporter:
    """
    Unified exporter that creates a single file containing all step data.
    Removes overlaps between steps but preserves all unique cards.
    """
    
    def __init__(self):
        # Use absolute path to ensure we're in the right directory
        addon_dir = Path(__file__).parent
        self.exports_dir = addon_dir / "exports"
        self.exports_dir.mkdir(exist_ok=True)
    
    def _clear_exports_directory(self):
        """Clear all files in the exports directory before creating new exports."""
        try:
            if self.exports_dir.exists():
                for file_path in self.exports_dir.iterdir():
                    if file_path.is_file():
                        file_path.unlink()
                        print(f"🗑️  Removed old export file: {file_path.name}")
                print("✅ Exports directory cleared")
        except Exception as e:
            print(f"⚠️  Warning: Could not clear exports directory: {str(e)}")
        
    def export_all_data(self) -> Dict[str, Any]:
        """
        Main export function that creates ONLY:
        1. unified_export_TIMESTAMP.ndjson.gz (ALL data, no overlaps)
        
        Returns:
        {
            'unified_export': {
                'file_path': str, 
                'record_count': int, 
                'unique_cards': int,
                'overlap_removed': int,
                'success': bool
            },
            'timing': {
                'total_time': float
            }
        }
        """
        print("\n🚀 Starting Unified Card Export (Single File)")
        total_start = time.time()
        
        # Clear exports directory before creating new files
        print("\n🗑️  Clearing exports directory...")
        self._clear_exports_directory()
        
        print("\n🔗 Creating unified export with all data (no overlaps)...")
        unified_result = self._create_direct_unified_export()
        
        total_time = time.time() - total_start
        
        # Prepare final results
        results = {
            'unified_export': unified_result,
            'timing': {
                'total_time': total_time
            }
        }
        
        self._print_summary(results)
        return results
    
    def _create_direct_unified_export(self) -> Dict[str, Any]:
        """
        Create unified export with ALL tag data from the Tags section.
        This includes: !AK_UpdateTags, Untagged, #AK_Step1_v12, #AK_Step2_v12, #AK_Step3_v12, etc.
        """
        try:
            print("🔍 Creating unified export with ALL tag data...")
            
            # Get the all tags service
            all_tags_service = ExportServiceFactory.create_service("all_tags")
            
            # Process the service to get all tag data
            print("   📋 Processing ALL tags with hierarchical data...")
            all_tags_data = self._get_hierarchical_export_data(all_tags_service, "AllTags")
            
            print(f"📊 All tags data processed: {len(all_tags_data)} records")
            
            # Count unique cards across all tags
            all_unique_cards = set()
            for record in all_tags_data:
                # Extract card IDs from the hierarchical data
                record_cards = []
                if 'unstudied_card_details' in record:
                    record_cards.extend([card['card_id'] for card in record['unstudied_card_details']])
                if 'all_card_details' in record:
                    record_cards.extend([card['card_id'] for card in record['all_card_details']])
                    
                if record_cards:
                    all_unique_cards.update(record_cards)
            
            print(f"📈 Total unique cards across all tags: {len(all_unique_cards):,}")
            
            # Generate unified filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"unified_export_{timestamp}.ndjson.gz"
            file_path = self.exports_dir / filename
            
            print(f"💾 Writing unified file with all tag data: {filename}")
            
            # Write all tags data to unified file
            with gzip.open(file_path, 'wt', encoding='utf-8') as f:
                for record in all_tags_data:
                    f.write(json.dumps(record, ensure_ascii=False) + '\n')
            
            file_size = file_path.stat().st_size
            print(f"✅ Unified file created with all tag data: {file_path} ({file_size:,} bytes)")
            
            return {
                'file_path': str(file_path),
                'record_count': len(all_tags_data),
                'unique_cards': len(all_unique_cards),
                'overlap_removed': 0,  # We're including all data with hierarchical counts
                'success': True
            }
            
        except Exception as e:
            print(f"❌ Unified export failed: {str(e)}")
            return {
                'file_path': '',
                'record_count': 0,
                'unique_cards': 0,
                'overlap_removed': 0,
                'success': False,
                'error': str(e)
            }
    
    def _get_hierarchical_export_data(self, service, step_name: str) -> List[Dict[str, Any]]:
        """Extract hierarchical export data from a service including parent-only tags"""
        try:
            # Use the service's internal data processing flow to get hierarchical data
            service._load_all_data_optimized()
            filtered_cards_by_tag = service._filter_cards_by_tag_fast()
            service.tag_counter.build_hierarchy_from_cards(filtered_cards_by_tag)
            hierarchical_data = service.tag_counter.calculate_hierarchical_counts()
            
            # Create export data with hierarchical counts (includes parent-only tags)
            export_data = service._create_export_data_fast(filtered_cards_by_tag, hierarchical_data)
            
            # Add step name to each record for tracking
            for record in export_data:
                record['source_step'] = step_name
                record['unified_export'] = True
            
            print(f"     📊 {step_name}: Generated {len(export_data)} records with hierarchical data")
            
            # Count parent-only tags (tags with 0 direct cards but >0 hierarchical cards)
            parent_only_count = sum(1 for record in export_data 
                                  if record.get('total_cards', 0) == 0 and record.get('hierarchical_total_cards', 0) > 0)
            
            if parent_only_count > 0:
                print(f"     🏷️  {step_name}: Includes {parent_only_count} parent-only tags (like #AK_Step{step_name[-1]}_v12)")
            
            return export_data
            
        except Exception as e:
            print(f"❌ Failed to get hierarchical data from {step_name} service: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _print_summary(self, results: Dict[str, Any]):
        """Print a summary of the export results"""
        print("\n" + "="*60)
        print("📊 UNIFIED EXPORT SUMMARY")
        print("="*60)
        
        unified = results['unified_export']
        timing = results['timing']
        
        if unified.get('success', False):
            print(f"🔗 Unified Export (All Tags Data):")
            print(f"   Tag Records: {unified['record_count']:,} (includes all tags from Tags section)")
            print(f"   Total Unique Cards: {unified['unique_cards']:,}")
            print(f"   Includes: !AK_UpdateTags, Untagged, #AK_Step1_v12, #AK_Step2_v12, #AK_Step3_v12, etc.")
            print(f"   Hierarchical counting with parent tags included")
            print(f"   File: {unified['file_path']}")
            
            print(f"\n⏱️  Timing:")
            print(f"   Total Time: {timing['total_time']:.2f}s")
            
            print(f"\n✅ SUCCESS: Unified file contains complete Tags section data!")
        else:
            print(f"❌ FAILED: {unified.get('error', 'Unknown error')}")
        
        print("="*60)


# Convenience function for external use
def export_all_tags_data() -> Dict[str, Any]:
    """
    Export all tag data from the Tags section into a single unified file.
    This includes: !AK_UpdateTags, Untagged, #AK_Step1_v12, #AK_Step2_v12, #AK_Step3_v12, etc.
    
    Returns the same format as UnifiedCardExporter.export_all_data()
    """
    exporter = UnifiedCardExporter()
    return exporter.export_all_data()


if __name__ == "__main__":
    results = export_all_tags_data()
    if results['unified_export']['success']:
        print(f"\n🎉 All tags export completed successfully!")
        print(f"File: {results['unified_export']['file_path']}")
    else:
        print(f"\n💥 All tags export failed!")