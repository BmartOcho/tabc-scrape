"""
SQLAlchemy models for restaurant data storage and enrichment pipeline
"""

from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, ForeignKey, JSON, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import json
from typing import Dict, List, Any

Base = declarative_base()

class Restaurant(Base):
    """Core restaurant data from TABC API"""
    __tablename__ = "restaurants"

    id = Column(String, primary_key=True, index=True)
    taxpayer_number = Column(String, index=True)
    taxpayer_name = Column(String)
    taxpayer_address = Column(String)
    taxpayer_city = Column(String)
    taxpayer_state = Column(String)
    taxpayer_zip = Column(String)
    taxpayer_county = Column(String)

    location_number = Column(String)
    location_name = Column(String, index=True)
    location_address = Column(String)
    location_city = Column(String)
    location_state = Column(String)
    location_zip = Column(String)
    location_county = Column(String)

    tabc_permit_number = Column(String, index=True)
    responsibility_begin_date = Column(String)
    responsibility_end_date = Column(String)
    obligation_end_date = Column(String)

    liquor_receipts = Column(Float, default=0.0)
    wine_receipts = Column(Float, default=0.0)
    beer_receipts = Column(Float, default=0.0)
    cover_charge_receipts = Column(Float, default=0.0)
    total_receipts = Column(Float, default=0.0)

    # Geocoding data
    latitude = Column(Float)
    longitude = Column(Float)
    geocoding_confidence = Column(Float, default=0.0)

    # Data quality and status
    data_quality_score = Column(Float, default=0.0)
    is_active = Column(Boolean, default=True)
    last_updated = Column(DateTime, default=func.now())
    created_at = Column(DateTime, default=func.now())

    # Relationships
    concept_classifications = relationship("ConceptClassification", back_populates="restaurant")
    population_data = relationship("PopulationData", back_populates="restaurant")
    square_footage_data = relationship("SquareFootageData", back_populates="restaurant")
    enrichment_jobs = relationship("EnrichmentJob", back_populates="restaurant")

    @property
    def full_address(self) -> str:
        """Get full address for geocoding"""
        return f"{self.location_address}, {self.location_city}, {self.location_state} {self.location_zip}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'id': self.id,
            'taxpayer_number': self.taxpayer_number,
            'taxpayer_name': self.taxpayer_name,
            'taxpayer_address': self.taxpayer_address,
            'taxpayer_city': self.taxpayer_city,
            'taxpayer_state': self.taxpayer_state,
            'taxpayer_zip': self.taxpayer_zip,
            'taxpayer_county': self.taxpayer_county,
            'location_number': self.location_number,
            'location_name': self.location_name,
            'location_address': self.location_address,
            'location_city': self.location_city,
            'location_state': self.location_state,
            'location_zip': self.location_zip,
            'location_county': self.location_county,
            'tabc_permit_number': self.tabc_permit_number,
            'responsibility_begin_date': self.responsibility_begin_date,
            'responsibility_end_date': self.responsibility_end_date,
            'obligation_end_date': self.obligation_end_date,
            'liquor_receipts': self.liquor_receipts,
            'wine_receipts': self.wine_receipts,
            'beer_receipts': self.beer_receipts,
            'cover_charge_receipts': self.cover_charge_receipts,
            'total_receipts': self.total_receipts,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'geocoding_confidence': self.geocoding_confidence,
            'data_quality_score': self.data_quality_score,
            'is_active': self.is_active,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'full_address': self.full_address
        }

# Indexes for Restaurant
Index('ix_restaurant_location_city', Restaurant.location_city)
Index('ix_restaurant_location_state', Restaurant.location_state)
Index('ix_restaurant_total_receipts', Restaurant.total_receipts)
Index('ix_restaurant_latitude', Restaurant.latitude)
Index('ix_restaurant_longitude', Restaurant.longitude)
Index('ix_restaurant_location', Restaurant.location_city, Restaurant.location_state)

class ConceptClassification(Base):
    """Restaurant concept classification results"""
    __tablename__ = "concept_classifications"

    id = Column(Integer, primary_key=True, index=True)
    restaurant_id = Column(String, ForeignKey("restaurants.id"), index=True)

    primary_concept = Column(String, index=True)
    secondary_concepts = Column(JSON)  # List of secondary concepts
    confidence = Column(Float)
    ai_confidence = Column(Float, default=0.0)

    # Classification metadata
    source = Column(String)  # 'ai_classification', 'rule_based', 'web_scraping'
    web_data_sources = Column(JSON)  # List of sources used
    keywords_found = Column(JSON)  # Keywords that led to classification
    price_range = Column(String)
    ambiance_indicators = Column(JSON)

    # Timestamps
    classified_at = Column(DateTime, default=func.now())
    last_updated = Column(DateTime, default=func.now())

    # Relationship
    restaurant = relationship("Restaurant", back_populates="concept_classifications")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'restaurant_id': self.restaurant_id,
            'primary_concept': self.primary_concept,
            'secondary_concepts': self.secondary_concepts or [],
            'confidence': self.confidence,
            'ai_confidence': self.ai_confidence,
            'source': self.source,
            'web_data_sources': self.web_data_sources or [],
            'keywords_found': self.keywords_found or [],
            'price_range': self.price_range,
            'ambiance_indicators': self.ambiance_indicators or [],
            'classified_at': self.classified_at.isoformat() if self.classified_at else None,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None
        }

# Indexes for ConceptClassification
Index('ix_concept_confidence', ConceptClassification.confidence)
Index('ix_concept_source', ConceptClassification.source)

class PopulationData(Base):
    """Population and demographic data for restaurant locations"""
    __tablename__ = "population_data"

    id = Column(Integer, primary_key=True, index=True)
    restaurant_id = Column(String, ForeignKey("restaurants.id"), index=True)

    # Population counts by radius
    population_1_mile = Column(Integer, default=0)
    population_3_mile = Column(Integer, default=0)
    population_5_mile = Column(Integer, default=0)
    population_10_mile = Column(Integer, default=0)

    # Drinking age population (21+)
    drinking_age_1_mile = Column(Integer, default=0)
    drinking_age_3_mile = Column(Integer, default=0)
    drinking_age_5_mile = Column(Integer, default=0)
    drinking_age_10_mile = Column(Integer, default=0)

    # Demographic data
    median_income_1_mile = Column(Float)
    median_age_1_mile = Column(Float)
    average_household_size_1_mile = Column(Float)

    # Metadata
    source = Column(String)  # 'census_api', 'zip_estimation', etc.
    confidence = Column(Float, default=0.0)
    census_data_available = Column(Boolean, default=False)

    # Timestamps
    analyzed_at = Column(DateTime, default=func.now())
    last_updated = Column(DateTime, default=func.now())

    # Relationship
    restaurant = relationship("Restaurant", back_populates="population_data")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'restaurant_id': self.restaurant_id,
            'population_1_mile': self.population_1_mile,
            'population_3_mile': self.population_3_mile,
            'population_5_mile': self.population_5_mile,
            'population_10_mile': self.population_10_mile,
            'drinking_age_1_mile': self.drinking_age_1_mile,
            'drinking_age_3_mile': self.drinking_age_3_mile,
            'drinking_age_5_mile': self.drinking_age_5_mile,
            'drinking_age_10_mile': self.drinking_age_10_mile,
            'median_income_1_mile': self.median_income_1_mile,
            'median_age_1_mile': self.median_age_1_mile,
            'average_household_size_1_mile': self.average_household_size_1_mile,
            'source': self.source,
            'confidence': self.confidence,
            'census_data_available': self.census_data_available,
            'analyzed_at': self.analyzed_at.isoformat() if self.analyzed_at else None,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None
        }

# Indexes for PopulationData
Index('ix_population_1_mile', PopulationData.population_1_mile)
Index('ix_population_3_mile', PopulationData.population_3_mile)
Index('ix_population_5_mile', PopulationData.population_5_mile)
Index('ix_population_10_mile', PopulationData.population_10_mile)

class SquareFootageData(Base):
    """Square footage data for restaurants"""
    __tablename__ = "square_footage_data"

    id = Column(Integer, primary_key=True, index=True)
    restaurant_id = Column(String, ForeignKey("restaurants.id"), index=True)

    square_footage = Column(Integer)
    source = Column(String)  # 'county_records', 'restaurant_website', etc.
    confidence = Column(Float)

    # Additional property details
    property_details = Column(JSON)
    source_url = Column(String)

    # Timestamps
    scraped_at = Column(DateTime, default=func.now())
    last_updated = Column(DateTime, default=func.now())

    # Relationship
    restaurant = relationship("Restaurant", back_populates="square_footage_data")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'restaurant_id': self.restaurant_id,
            'square_footage': self.square_footage,
            'source': self.source,
            'confidence': self.confidence,
            'property_details': self.property_details,
            'source_url': self.source_url,
            'scraped_at': self.scraped_at.isoformat() if self.scraped_at else None,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None
        }

# Indexes for SquareFootageData
Index('ix_sqft_square_footage', SquareFootageData.square_footage)
Index('ix_sqft_confidence', SquareFootageData.confidence)

class EnrichmentJob(Base):
    """Tracks data enrichment jobs and their status"""
    __tablename__ = "enrichment_jobs"

    id = Column(Integer, primary_key=True, index=True)
    restaurant_id = Column(String, ForeignKey("restaurants.id"), index=True)

    # Job type and status
    job_type = Column(String)  # 'concept_classification', 'population_analysis', 'square_footage', 'full_enrichment'
    status = Column(String)  # 'pending', 'running', 'completed', 'failed'
    progress = Column(Integer, default=0)  # 0-100%

    # Job metadata
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    error_message = Column(Text)

    # Configuration
    job_config = Column(JSON)  # Configuration parameters for the job

    # Results summary
    results_summary = Column(JSON)

    # Relationship
    restaurant = relationship("Restaurant", back_populates="enrichment_jobs")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'restaurant_id': self.restaurant_id,
            'job_type': self.job_type,
            'status': self.status,
            'progress': self.progress,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'error_message': self.error_message,
            'job_config': self.job_config,
            'results_summary': self.results_summary
        }

# Indexes for EnrichmentJob
Index('ix_job_status', EnrichmentJob.status)
Index('ix_job_type', EnrichmentJob.job_type)

class DataQualityMetrics(Base):
    """Data quality metrics and validation results"""
    __tablename__ = "data_quality_metrics"

    id = Column(Integer, primary_key=True, index=True)
    restaurant_id = Column(String, ForeignKey("restaurants.id"), index=True)

    # Quality scores (0.0 to 1.0)
    overall_quality_score = Column(Float, default=0.0)
    completeness_score = Column(Float, default=0.0)
    accuracy_score = Column(Float, default=0.0)
    consistency_score = Column(Float, default=0.0)
    timeliness_score = Column(Float, default=0.0)

    # Validation results
    validation_errors = Column(JSON)  # List of validation errors
    validation_warnings = Column(JSON)  # List of validation warnings

    # Data source quality
    source_reliability_score = Column(Float, default=0.0)
    data_freshness_days = Column(Integer)  # How old the data is

    # Timestamps
    assessed_at = Column(DateTime, default=func.now())
    last_updated = Column(DateTime, default=func.now())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'restaurant_id': self.restaurant_id,
            'overall_quality_score': self.overall_quality_score,
            'completeness_score': self.completeness_score,
            'accuracy_score': self.accuracy_score,
            'consistency_score': self.consistency_score,
            'timeliness_score': self.timeliness_score,
            'validation_errors': self.validation_errors or [],
            'validation_warnings': self.validation_warnings or [],
            'source_reliability_score': self.source_reliability_score,
            'data_freshness_days': self.data_freshness_days,
            'assessed_at': self.assessed_at.isoformat() if self.assessed_at else None,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None
        }

# Indexes for DataQualityMetrics
Index('ix_quality_overall_score', DataQualityMetrics.overall_quality_score)