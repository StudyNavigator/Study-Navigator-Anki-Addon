from typing import Dict, Any
from .base_export_service import BaseExportService

class AllTagsExportService(BaseExportService):
    """Export service for ALL tag content - NO filtering, includes everything"""
    
    def get_service_name(self) -> str:
        return "All Tags Export"
    
    def get_filter_criteria(self) -> Dict[str, Any]:
        return {
            # No include_patterns = include ALL tags
            "include_patterns": [],
            # NO exclude_patterns = include EVERYTHING including instruction cards
            "exclude_patterns": []
        }
