"""
Data enrichment pipeline coordinator for restaurant data processing
"""

import logging
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import asyncio
from datetime import datetime

from ..scraping.square_footage import SquareFootageScraper
from ..scraping.concept_classifier import EnhancedRestaurantConceptClassifier
from ..analysis.population import PopulationAnalyzer
from .database import DatabaseManager
from sqlalchemy.sql import func

logger = logging.getLogger(__name__)

@dataclass
class EnrichmentResult:
    """Result of data enrichment for a restaurant"""
    restaurant_id: str
    success: bool
    errors: List[str]
    warnings: List[str]
    processing_time: float
    data_collected: Dict[str, Any]

@dataclass
class PipelineStats:
    """Statistics for pipeline execution"""
    total_restaurants: int
    successful_enrichments: int
    failed_enrichments: int
    total_processing_time: float
    average_time_per_restaurant: float
    data_sources_used: Dict[str, int]
    error_summary: Dict[str, int]

class DataEnrichmentPipeline:
    """Coordinates the complete data enrichment pipeline"""

    def __init__(self, database_manager: DatabaseManager):
        self.db = database_manager

        # Initialize all data collection components (lazy loading to avoid circular imports)
        self._api_client = None
        self.square_footage_scraper = SquareFootageScraper()
        self.concept_classifier = EnhancedRestaurantConceptClassifier()
        self.population_analyzer = PopulationAnalyzer()

        # Pipeline configuration
        self.batch_size = 10
        self.max_concurrent_jobs = 3
        self.enable_square_footage_scraping = True
        self.enable_concept_classification = True
        self.enable_population_analysis = True

    @property
    def api_client(self):
        """Lazy load API client to avoid circular imports"""
        if self._api_client is None:
            from ..data.api_client import TexasComptrollerAPI
            self._api_client = TexasComptrollerAPI()
        return self._api_client

        # Pipeline configuration
        self.batch_size = 10
        self.max_concurrent_jobs = 3
        self.enable_square_footage_scraping = True
        self.enable_concept_classification = True
        self.enable_population_analysis = True

    async def enrich_single_restaurant(self, restaurant_id: str) -> EnrichmentResult:
        """
        Enrich a single restaurant with all available data

        Args:
            restaurant_id: Restaurant ID to enrich

        Returns:
            EnrichmentResult with results and metadata
        """
        start_time = time.time()
        errors = []
        warnings = []
        data_collected = {}

        logger.info(f"Starting enrichment for restaurant {restaurant_id}")

        try:
            # Get restaurant data
            restaurant = self.db.get_restaurant_by_id(restaurant_id)
            if not restaurant:
                errors.append(f"Restaurant {restaurant_id} not found in database")
                return EnrichmentResult(
                    restaurant_id=restaurant_id,
                    success=False,
                    errors=errors,
                    warnings=warnings,
                    processing_time=time.time() - start_time,
                    data_collected={}
                )

            # 1. Concept Classification
            if self.enable_concept_classification:
                try:
                    logger.info(f"Classifying concept for {restaurant.location_name}")
                    classification = await self.concept_classifier.classify_restaurant(
                        restaurant.location_name,
                        restaurant.full_address
                    )

                    if classification.confidence > 0.3:  # Minimum confidence threshold
                        self.db.store_concept_classification(restaurant_id, classification.__dict__)
                        data_collected['concept_classification'] = True
                        logger.info(f"Concept classification completed: {classification.primary_concept}")
                    else:
                        warnings.append(f"Low confidence in concept classification: {classification.confidence:.2f}")

                except Exception as e:
                    errors.append(f"Concept classification failed: {e}")
                    logger.error(f"Concept classification error: {e}")

            # 2. Population Analysis
            if self.enable_population_analysis:
                try:
                    logger.info(f"Analyzing population data for {restaurant.location_name}")
                    population_result = await self.population_analyzer.analyze_location(
                        restaurant.location_name,
                        restaurant.full_address
                    )

                    if population_result.census_data_available:
                        self.db.store_population_data(restaurant_id, population_result.__dict__)
                        data_collected['population_analysis'] = True
                        logger.info(f"Population analysis completed: {population_result.population_1_mile:,}"," people within 1 mile")
                    else:
                        warnings.append("Population analysis returned no data")

                except Exception as e:
                    errors.append(f"Population analysis failed: {e}")
                    logger.error(f"Population analysis error: {e}")

            # 3. Square Footage Scraping
            if self.enable_square_footage_scraping:
                try:
                    logger.info(f"Scraping square footage for {restaurant.location_name}")
                    sqft_result = await self.square_footage_scraper.scrape_square_footage(
                        restaurant.location_name,
                        restaurant.full_address,
                        restaurant.location_county
                    )

                    if sqft_result.square_footage:
                        self.db.store_square_footage_data(restaurant_id, sqft_result.__dict__)
                        data_collected['square_footage'] = True
                        logger.info(f"Square footage scraping completed: {sqft_result.square_footage:,}"," sq ft")
                    else:
                        warnings.append("No square footage data found")

                except Exception as e:
                    errors.append(f"Square footage scraping failed: {e}")
                    logger.error(f"Square footage scraping error: {e}")

            processing_time = time.time() - start_time
            success = len(errors) == 0

            logger.info(f"Enrichment completed for {restaurant_id} in {processing_time:.2f}s")

            return EnrichmentResult(
                restaurant_id=restaurant_id,
                success=success,
                errors=errors,
                warnings=warnings,
                processing_time=processing_time,
                data_collected=data_collected
            )

        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Unexpected error enriching restaurant {restaurant_id}: {e}")
            return EnrichmentResult(
                restaurant_id=restaurant_id,
                success=False,
                errors=[f"Unexpected error: {e}"],
                warnings=warnings,
                processing_time=processing_time,
                data_collected={}
            )

    async def enrich_restaurants_batch(self, restaurant_ids: List[str]) -> List[EnrichmentResult]:
        """
        Enrich a batch of restaurants

        Args:
            restaurant_ids: List of restaurant IDs to enrich

        Returns:
            List of EnrichmentResult objects
        """
        logger.info(f"Starting batch enrichment for {len(restaurant_ids)} restaurants")

        # Use asyncio.gather for concurrent processing
        tasks = [self.enrich_single_restaurant(rid) for rid in restaurant_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle exceptions
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error enriching restaurant {restaurant_ids[i]}: {result}")
                final_results.append(EnrichmentResult(
                    restaurant_id=restaurant_ids[i],
                    success=False,
                    errors=[f"Exception: {result}"],
                    warnings=[],
                    processing_time=0.0,
                    data_collected={}
                ))
            else:
                final_results.append(result)

        return final_results

    async def run_full_enrichment_pipeline(self, limit: Optional[int] = None) -> PipelineStats:
        """
        Run the complete enrichment pipeline on all restaurants

        Args:
            limit: Maximum number of restaurants to process (None for all)

        Returns:
            PipelineStats with execution statistics
        """
        logger.info("Starting full enrichment pipeline")

        # Get restaurants to enrich
        restaurants_df = self.db.get_restaurants_dataframe(limit=limit)
        if restaurants_df.empty:
            logger.warning("No restaurants found for enrichment")
            return PipelineStats(0, 0, 0, 0.0, 0.0, {}, {})

        restaurant_ids = restaurants_df['id'].tolist()
        logger.info(f"Enriching {len(restaurant_ids)} restaurants")

        # Process in batches
        all_results = []
        start_time = time.time()

        for i in range(0, len(restaurant_ids), self.batch_size):
            batch_ids = restaurant_ids[i:i + self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1}: restaurants {i+1}-{min(i+self.batch_size, len(restaurant_ids))}")

            batch_results = await self.enrich_restaurants_batch(batch_ids)
            all_results.extend(batch_results)

            # Progress update
            completed = len(all_results)
            success_count = sum(1 for r in all_results if r.success)
            logger.info(f"Progress: {completed}/{len(restaurant_ids)} completed, {success_count} successful")

        total_time = time.time() - start_time

        # Calculate statistics
        successful = sum(1 for r in all_results if r.success)
        failed = len(all_results) - successful

        # Data sources used
        data_sources = {}
        for result in all_results:
            for data_type in result.data_collected.keys():
                data_sources[data_type] = data_sources.get(data_type, 0) + 1

        # Error summary
        error_summary = {}
        for result in all_results:
            for error in result.errors:
                error_type = error.split(':')[0] if ':' in error else 'Unknown'
                error_summary[error_type] = error_summary.get(error_type, 0) + 1

        avg_time = total_time / len(restaurant_ids) if restaurant_ids else 0

        stats = PipelineStats(
            total_restaurants=len(restaurant_ids),
            successful_enrichments=successful,
            failed_enrichments=failed,
            total_processing_time=total_time,
            average_time_per_restaurant=avg_time,
            data_sources_used=data_sources,
            error_summary=error_summary
        )

        logger.info(f"Pipeline completed: {successful}/{len(restaurant_ids)} successful in {total_time:.2f}s")
        return stats

    def get_enrichment_status(self) -> Dict[str, Any]:
        """
        Get current enrichment status and statistics

        Returns:
            Dictionary with enrichment status information
        """
        try:
            # Get database statistics
            db_stats = self.db.get_enrichment_stats()

            # Get active jobs
            with self.db.get_session() as session:
                from .models import EnrichmentJob

                active_jobs = session.query(EnrichmentJob).filter(
                    EnrichmentJob.status.in_(['pending', 'running'])
                ).all()

                recent_jobs = session.query(EnrichmentJob).filter(
                    EnrichmentJob.status.in_(['completed', 'failed'])
                ).order_by(EnrichmentJob.completed_at.desc()).limit(10).all()

            return {
                'database_stats': db_stats,
                'active_jobs': len(active_jobs),
                'recent_jobs': [
                    {
                        'id': job.id,
                        'restaurant_id': job.restaurant_id,
                        'job_type': job.job_type,
                        'status': job.status,
                        'completed_at': job.completed_at.isoformat() if job.completed_at else None
                    }
                    for job in recent_jobs
                ],
                'pipeline_config': {
                    'batch_size': self.batch_size,
                    'max_concurrent_jobs': self.max_concurrent_jobs,
                    'enable_square_footage_scraping': self.enable_square_footage_scraping,
                    'enable_concept_classification': self.enable_concept_classification,
                    'enable_population_analysis': self.enable_population_analysis
                }
            }

        except Exception as e:
            logger.error(f"Error getting enrichment status: {e}")
            return {'error': str(e)}

    def validate_enriched_data(self, restaurant_id: str) -> Dict[str, Any]:
        """
        Validate enriched data for a restaurant

        Args:
            restaurant_id: Restaurant ID to validate

        Returns:
            Validation results
        """
        validation_results = {
            'restaurant_id': restaurant_id,
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'quality_score': 0.0
        }

        try:
            restaurant = self.db.get_restaurant_by_id(restaurant_id)
            if not restaurant:
                validation_results['errors'].append("Restaurant not found")
                validation_results['is_valid'] = False
                return validation_results

            # Check geocoding
            if not restaurant.latitude or not restaurant.longitude:
                validation_results['warnings'].append("Restaurant not geocoded")
                validation_results['quality_score'] -= 0.2

            # Check concept classification
            with self.db.get_session() as session:
                from .models import ConceptClassification
                concept = session.query(ConceptClassification).filter_by(restaurant_id=restaurant_id).first()

                if not concept:
                    validation_results['warnings'].append("No concept classification")
                    validation_results['quality_score'] -= 0.3
                elif concept.confidence < 0.5:
                    validation_results['warnings'].append(f"Low confidence concept classification: {concept.confidence:.2f}")

            # Check population data
            with self.db.get_session() as session:
                from .models import PopulationData
                population = session.query(PopulationData).filter_by(restaurant_id=restaurant_id).first()

                if not population:
                    validation_results['warnings'].append("No population analysis")
                    validation_results['quality_score'] -= 0.2
                elif population.population_1_mile == 0:
                    validation_results['warnings'].append("Zero population within 1 mile")

            # Check square footage
            with self.db.get_session() as session:
                from .models import SquareFootageData
                sqft = session.query(SquareFootageData).filter_by(restaurant_id=restaurant_id).first()

                if not sqft or not sqft.square_footage:
                    validation_results['warnings'].append("No square footage data")
                    validation_results['quality_score'] -= 0.1

            # Calculate final quality score
            validation_results['quality_score'] = max(0.0, validation_results['quality_score'] + 1.0)

            # Overall validity
            if len(validation_results['errors']) > 0:
                validation_results['is_valid'] = False

            return validation_results

        except Exception as e:
            validation_results['errors'].append(f"Validation error: {e}")
            validation_results['is_valid'] = False
            return validation_results

    def export_enriched_data(self, format: str = 'json', filepath: Optional[str] = None) -> str:
        """
        Export all enriched restaurant data

        Args:
            format: Export format ('json' or 'csv')
            filepath: Export filepath (optional)

        Returns:
            Path to exported file
        """
        if format.lower() == 'json':
            return self.db.export_to_json(filepath)
        elif format.lower() == 'csv':
            return self.db.export_to_csv(filepath)
        else:
            raise ValueError(f"Unsupported export format: {format}")

    def create_enrichment_job_for_restaurant(self, restaurant_id: str, job_type: str = 'full_enrichment') -> int:
        """
        Create an enrichment job for a specific restaurant

        Args:
            restaurant_id: Restaurant ID
            job_type: Type of enrichment job

        Returns:
            Job ID
        """
        job_config = {
            'restaurant_id': restaurant_id,
            'job_type': job_type,
            'created_at': datetime.now().isoformat(),
            'pipeline_version': '2.0'
        }

        return self.db.create_enrichment_job(restaurant_id, job_type, job_config)

    async def process_enrichment_job(self, job_id: int) -> bool:
        """
        Process a specific enrichment job

        Args:
            job_id: Job ID to process

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db.get_session() as session:
                from .models import EnrichmentJob

                job = session.query(EnrichmentJob).filter_by(id=job_id).first()
                if not job:
                    logger.error(f"Job {job_id} not found")
                    return False

                # Update job status to running
                job.status = 'running'
                job.started_at = func.now()
                session.commit()

                logger.info(f"Processing enrichment job {job_id} for restaurant {job.restaurant_id}")

                # Process based on job type
                if job.job_type == 'full_enrichment':
                    result = await self.enrich_single_restaurant(job.restaurant_id)
                else:
                    # Handle other job types
                    result = EnrichmentResult(
                        restaurant_id=job.restaurant_id,
                        success=False,
                        errors=[f"Unsupported job type: {job.job_type}"],
                        warnings=[],
                        processing_time=0.0,
                        data_collected={}
                    )

                # Update job with results
                if result.success:
                    self.db.update_enrichment_job_status(
                        job_id,
                        'completed',
                        100,
                        results_summary={
                            'data_collected': result.data_collected,
                            'processing_time': result.processing_time
                        }
                    )
                else:
                    self.db.update_enrichment_job_status(
                        job_id,
                        'failed',
                        0,
                        f"Enrichment failed: {'; '.join(result.errors)}",
                        {'errors': result.errors, 'warnings': result.warnings}
                    )

                return result.success

        except Exception as e:
            logger.error(f"Error processing job {job_id}: {e}")
            self.db.update_enrichment_job_status(job_id, 'failed', 0, str(e))
            return False