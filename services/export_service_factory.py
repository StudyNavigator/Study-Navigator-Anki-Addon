from typing import List
from .base_export_service import BaseExportService
from .all_tags_export_service import AllTagsExportService

class ExportServiceFactory:
    """Factory for creating export services"""
    
    _services = {
        "all_tags": AllTagsExportService,
    }
    
    @classmethod
    def create_service(cls, service_type: str) -> BaseExportService:
        """Create a service instance"""
        if service_type not in cls._services:
            available = list(cls._services.keys())
            raise ValueError(f"Unknown service type: {service_type}. Available: {available}")
        
        return cls._services[service_type]()

