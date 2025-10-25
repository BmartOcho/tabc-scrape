import asyncio
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class WorkflowManager:
    """Manage data pipeline workflows"""
    
    def __init__(self):
        self.active_jobs = {}
    
    async def run_full_pipeline(self, limit: int = 100) -> Dict[str, Any]:
        """Run the complete data pipeline"""
        from .data.api_client import TexasComptrollerAPI
        from .storage.database import DatabaseManager
        from .storage.enrichment_pipeline import DataEnrichmentPipeline
        
        results = {
            'fetch': None,
            'enrich': None,
            'status': 'success',
            'errors': []
        }
        
        try:
            # Step 1: Fetch data
            logger.info("Step 1: Fetching restaurant data...")
            api_client = TexasComptrollerAPI()
            db_manager = DatabaseManager()
            
            restaurants = await api_client.get_all_restaurants(limit=limit)
            stored_count = db_manager.store_restaurants([r.__dict__ for r in restaurants])
            results['fetch'] = {'count': stored_count}
            
            # Step 2: Enrich data
            logger.info("Step 2: Enriching data...")
            pipeline = DataEnrichmentPipeline(db_manager)
            pipeline.batch_size = 5  # Smaller batches for Replit
            
            stats = await pipeline.run_full_enrichment_pipeline(limit=min(limit, 10))
            results['enrich'] = {
                'successful': stats.successful_enrichments,
                'failed': stats.failed_enrichments
            }
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            results['status'] = 'error'
            results['errors'].append(str(e))
        
        return results