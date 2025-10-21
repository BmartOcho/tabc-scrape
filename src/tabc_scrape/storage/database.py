"""
Enhanced database management with SQLAlchemy and enrichment pipeline
"""

import logging
import json
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import pandas as pd
from contextlib import contextmanager

from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, Text, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional

from ..config import config
from .models import Base, Restaurant, ConceptClassification, PopulationData, SquareFootageData, EnrichmentJob, DataQualityMetrics

logger = logging.getLogger(__name__)

@dataclass
class DatabaseManager:
    """Enhanced database manager using SQLAlchemy with enrichment pipeline support"""

    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or config.database.url
        # Mask credentials in logs
        masked_url = self._mask_database_url(self.database_url)
        self.engine = create_engine(self.database_url, echo=config.database.echo)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        logger.info(f"Initialized database with URL: {masked_url}")

        # Create tables
        self._create_tables()

        # Ensure data directory exists for file exports
        self._ensure_data_directory()

    def _mask_database_url(self, url: str) -> str:
        """Mask credentials in database URL for logging"""
        if '://' in url and '@' in url:
            scheme, rest = url.split('://', 1)
            if '@' in rest:
                user_pass, host_db = rest.split('@', 1)
                return f"{scheme}://***:***@{host_db}"
        return url

    def _ensure_data_directory(self):
        """Ensure data directory exists"""
        os.makedirs("data", exist_ok=True)

    def _create_tables(self):
        """Create all database tables"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating database tables: {e}")
            raise

    @contextmanager
    def get_session(self):
        """Context manager for database sessions"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()

    def store_restaurants(self, restaurants: List[Dict[str, Any]]) -> int:
        """
        Store restaurant data in the database

        Args:
            restaurants: List of restaurant dictionaries

        Returns:
            Number of records stored
        """
        stored_count = 0

        with self.get_session() as session:
            for restaurant_data in restaurants:
                try:
                    # Check if restaurant already exists
                    existing = session.query(Restaurant).filter_by(id=restaurant_data['id']).first()

                    if existing:
                        # Update existing record
                        for key, value in restaurant_data.items():
                            if hasattr(existing, key):
                                setattr(existing, key, value)
                        existing.last_updated = func.now()
                    else:
                        # Create new restaurant record
                        restaurant = Restaurant(**restaurant_data)
                        session.add(restaurant)

                    stored_count += 1

                except Exception as e:
                    logger.error(f"Error storing restaurant {restaurant_data.get('id', 'unknown')}: {e}")
                    continue

            logger.info(f"Successfully stored/updated {stored_count} restaurant records")
            return stored_count

    def get_restaurant_by_id(self, restaurant_id: str) -> Optional[Restaurant]:
        """
        Get a specific restaurant by ID

        Args:
            restaurant_id: Restaurant ID

        Returns:
            Restaurant object or None if not found
        """
        with self.get_session() as session:
            return session.query(Restaurant).filter_by(id=restaurant_id).first()

    def get_restaurants_dataframe(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get all restaurants as a pandas DataFrame

        Args:
            limit: Maximum number of records to return

        Returns:
            DataFrame with restaurant data
        """
        with self.get_session() as session:
            query = session.query(Restaurant)

            if limit:
                restaurants = query.limit(limit).all()
            else:
                restaurants = query.all()

            if not restaurants:
                logger.info("No restaurant records found")
                return pd.DataFrame()

            # Convert to list of dictionaries
            data = [restaurant.to_dict() for restaurant in restaurants]
            df = pd.DataFrame(data)

            logger.info(f"Retrieved {len(df)} restaurant records from database")
            return df

    def store_concept_classification(self, restaurant_id: str, classification_data: Dict[str, Any]) -> bool:
        """
        Store concept classification data

        Args:
            restaurant_id: Restaurant ID
            classification_data: Classification results

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_session() as session:
                # Remove existing classifications for this restaurant
                session.query(ConceptClassification).filter_by(restaurant_id=restaurant_id).delete()

                # Create new classification record
                classification = ConceptClassification(
                    restaurant_id=restaurant_id,
                    **classification_data
                )
                session.add(classification)

                logger.info(f"Stored concept classification for restaurant {restaurant_id}")
                return True

        except Exception as e:
            logger.error(f"Error storing concept classification: {e}")
            return False

    def store_population_data(self, restaurant_id: str, population_data: Dict[str, Any]) -> bool:
        """
        Store population analysis data

        Args:
            restaurant_id: Restaurant ID
            population_data: Population analysis results

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_session() as session:
                # Remove existing population data for this restaurant
                session.query(PopulationData).filter_by(restaurant_id=restaurant_id).delete()

                # Create new population data record
                pop_data = PopulationData(
                    restaurant_id=restaurant_id,
                    **population_data
                )
                session.add(pop_data)

                logger.info(f"Stored population data for restaurant {restaurant_id}")
                return True

        except Exception as e:
            logger.error(f"Error storing population data: {e}")
            return False

    def store_square_footage_data(self, restaurant_id: str, sqft_data: Dict[str, Any]) -> bool:
        """
        Store square footage data

        Args:
            restaurant_id: Restaurant ID
            sqft_data: Square footage scraping results

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_session() as session:
                # Remove existing square footage data for this restaurant
                session.query(SquareFootageData).filter_by(restaurant_id=restaurant_id).delete()

                # Create new square footage data record
                sqft_record = SquareFootageData(
                    restaurant_id=restaurant_id,
                    **sqft_data
                )
                session.add(sqft_record)

                logger.info(f"Stored square footage data for restaurant {restaurant_id}")
                return True

        except Exception as e:
            logger.error(f"Error storing square footage data: {e}")
            return False

    def create_enrichment_job(self, restaurant_id: str, job_type: str, job_config: Optional[Dict[str, Any]] = None) -> int:
        """
        Create a new enrichment job

        Args:
            restaurant_id: Restaurant ID
            job_type: Type of enrichment job
            job_config: Job configuration parameters

        Returns:
            Job ID
        """
        try:
            with self.get_session() as session:
                job = EnrichmentJob(
                    restaurant_id=restaurant_id,
                    job_type=job_type,
                    status='pending',
                    job_config=job_config or {}
                )
                session.add(job)
                session.flush()  # Get the job ID

                logger.info(f"Created enrichment job {job.id} for restaurant {restaurant_id}")
                return job.id

        except Exception as e:
            logger.error(f"Error creating enrichment job: {e}")
            return 0

    def update_enrichment_job_status(self, job_id: int, status: str, progress: Optional[int] = None, error_message: Optional[str] = None, results_summary: Optional[Dict] = None):
        """
        Update enrichment job status

        Args:
            job_id: Job ID
            status: New status
            progress: Progress percentage (0-100)
            error_message: Error message if failed
            results_summary: Summary of results
        """
        try:
            with self.get_session() as session:
                job = session.query(EnrichmentJob).filter_by(id=job_id).first()

                if not job:
                    logger.error(f"Enrichment job {job_id} not found")
                    return

                job.status = status
                if progress is not None:
                    job.progress = progress

                if status == 'completed':
                    job.completed_at = func.now()
                elif status == 'failed':
                    job.completed_at = func.now()
                    job.error_message = error_message

                if results_summary:
                    job.results_summary = results_summary

                logger.info(f"Updated enrichment job {job_id} status to {status}")

        except Exception as e:
            logger.error(f"Error updating enrichment job {job_id}: {e}")

    def get_enriched_restaurants_dataframe(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get restaurants with all enrichment data as a pandas DataFrame

        Args:
            limit: Maximum number of records to return

        Returns:
            DataFrame with enriched restaurant data
        """
        with self.get_session() as session:
            # Join restaurants with all enrichment data
            query = session.query(
                Restaurant,
                ConceptClassification,
                PopulationData,
                SquareFootageData
            ).outerjoin(
                ConceptClassification, Restaurant.id == ConceptClassification.restaurant_id
            ).outerjoin(
                PopulationData, Restaurant.id == PopulationData.restaurant_id
            ).outerjoin(
                SquareFootageData, Restaurant.id == SquareFootageData.restaurant_id
            )

            if limit:
                results = query.limit(limit).all()
            else:
                results = query.all()

            if not results:
                logger.info("No enriched restaurant records found")
                return pd.DataFrame()

            # Convert to list of dictionaries
            data = []
            for restaurant, concept, population, sqft in results:
                row = restaurant.to_dict()

                # Add concept data
                if concept:
                    row.update({
                        'concept_primary': concept.primary_concept,
                        'concept_secondary': concept.secondary_concepts or [],
                        'concept_confidence': concept.confidence,
                        'concept_source': concept.source
                    })
                else:
                    row.update({
                        'concept_primary': None,
                        'concept_secondary': [],
                        'concept_confidence': 0.0,
                        'concept_source': None
                    })

                # Add population data
                if population:
                    pop_dict = population.to_dict()
                    for key, value in pop_dict.items():
                        if key != 'id' and key != 'restaurant_id':
                            row[f"population_{key}"] = value
                else:
                    # Add default population columns
                    for radius in [1, 3, 5, 10]:
                        row[f"population_{radius}_mile"] = 0
                        row[f"drinking_age_{radius}_mile"] = 0

                # Add square footage data
                if sqft:
                    row.update({
                        'square_footage': sqft.square_footage,
                        'square_footage_source': sqft.source,
                        'square_footage_confidence': sqft.confidence
                    })
                else:
                    row.update({
                        'square_footage': None,
                        'square_footage_source': None,
                        'square_footage_confidence': 0.0
                    })

                data.append(row)

            df = pd.DataFrame(data)
            logger.info(f"Retrieved {len(df)} enriched restaurant records from database")
            return df

    def get_enrichment_stats(self) -> Dict[str, Any]:
        """
        Get statistics about data enrichment

        Returns:
            Dictionary with enrichment statistics
        """
        with self.get_session() as session:
            total_restaurants = session.query(Restaurant).count()
            restaurants_with_concepts = session.query(ConceptClassification).distinct(ConceptClassification.restaurant_id).count()
            restaurants_with_population = session.query(PopulationData).distinct(PopulationData.restaurant_id).count()
            restaurants_with_sqft = session.query(SquareFootageData).distinct(SquareFootageData.restaurant_id).count()

            # Active enrichment jobs
            active_jobs = session.query(EnrichmentJob).filter(
                EnrichmentJob.status.in_(['pending', 'running'])
            ).count()

            completed_jobs = session.query(EnrichmentJob).filter_by(status='completed').count()
            failed_jobs = session.query(EnrichmentJob).filter_by(status='failed').count()

            return {
                'total_restaurants': total_restaurants,
                'restaurants_with_concept_classification': restaurants_with_concepts,
                'restaurants_with_population_data': restaurants_with_population,
                'restaurants_with_square_footage': restaurants_with_sqft,
                'enrichment_coverage': {
                    'concept_classification': restaurants_with_concepts / total_restaurants if total_restaurants > 0 else 0,
                    'population_analysis': restaurants_with_population / total_restaurants if total_restaurants > 0 else 0,
                    'square_footage': restaurants_with_sqft / total_restaurants if total_restaurants > 0 else 0
                },
                'enrichment_jobs': {
                    'active': active_jobs,
                    'completed': completed_jobs,
                    'failed': failed_jobs,
                    'total': active_jobs + completed_jobs + failed_jobs
                }
            }

    def export_to_json(self, filepath: Optional[str] = None) -> str:
        """
        Export enriched restaurant data to JSON file

        Args:
            filepath: Path to export file (optional)

        Returns:
            Path to exported file
        """
        if not filepath:
            filepath = "data/enriched_restaurants_export.json"

        try:
            df = self.get_enriched_restaurants_dataframe()
            df.to_json(filepath, orient='records', indent=2)
            logger.info(f"Exported {len(df)} enriched restaurant records to {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Error exporting to JSON: {e}")
            raise

    def export_to_csv(self, filepath: Optional[str] = None) -> str:
        """
        Export enriched restaurant data to CSV file

        Args:
            filepath: Path to export file (optional)

        Returns:
            Path to exported file
        """
        if not filepath:
            filepath = "data/enriched_restaurants_export.csv"

        try:
            df = self.get_enriched_restaurants_dataframe()
            df.to_csv(filepath, index=False)
            logger.info(f"Exported {len(df)} enriched restaurant records to {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            raise

    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
                return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False