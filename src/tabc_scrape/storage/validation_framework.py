"""
Comprehensive Data Validation and Quality Assessment Framework
"""

import logging
import re
import json
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# Compile regex patterns once at module level for reuse
_ZIP_PATTERN = re.compile(r"^\d{5}(-\d{4})?$")
_STREET_SUFFIX_PATTERNS = {}  # Will be populated lazily
_VALID_STATES_SET = frozenset([
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
])

@dataclass
class ValidationRule:
    """Defines a validation rule for data quality checks"""
    name: str
    description: str
    field: str
    rule_type: str  # 'range', 'format', 'completeness', 'consistency', 'custom'
    parameters: Dict[str, Any]
    severity: str = 'error'  # 'error', 'warning', 'info'
    enabled: bool = True

@dataclass
class ValidationResult:
    """Result of a validation check"""
    rule_name: str
    field: str
    is_valid: bool
    severity: str
    message: str
    actual_value: Any = None
    expected_value: Any = None
    record_id: Optional[str] = None

@dataclass
class QualityReport:
    """Comprehensive quality assessment report"""
    total_records: int
    validation_results: List[ValidationResult]
    quality_score: float
    completeness_score: float
    accuracy_score: float
    consistency_score: float
    timeliness_score: float

    # Issue summaries
    errors_by_field: Dict[str, int] = field(default_factory=dict)
    warnings_by_field: Dict[str, int] = field(default_factory=dict)
    errors_by_type: Dict[str, int] = field(default_factory=dict)

    # Data insights
    outlier_records: List[str] = field(default_factory=list)
    duplicate_records: List[str] = field(default_factory=list)
    missing_data_patterns: Dict[str, List[str]] = field(default_factory=dict)

    generated_at: datetime = field(default_factory=datetime.now)

class ValidationEngine:
    """Core validation engine for data quality assessment"""

    def __init__(self):
        self.validation_rules = []
        self._initialize_default_rules()

    def _initialize_default_rules(self):
        """Initialize default validation rules for restaurant data"""

        # Restaurant data validation rules
        self.validation_rules = [
            # Completeness rules
            ValidationRule(
                name="required_location_name",
                description="Location name is required",
                field="location_name",
                rule_type="completeness",
                parameters={"required": True},
                severity="error"
            ),
            ValidationRule(
                name="required_address",
                description="Address information is required",
                field="location_address",
                rule_type="completeness",
                parameters={"required": True},
                severity="error"
            ),
            ValidationRule(
                name="required_receipts",
                description="Receipt data should be present",
                field="total_receipts",
                rule_type="completeness",
                parameters={"required": True},
                severity="warning"
            ),

            # Format validation rules
            ValidationRule(
                name="valid_zip_format",
                description="ZIP code must be 5 digits",
                field="location_zip",
                rule_type="format",
                parameters={"pattern": r"^\d{5}(-\d{4})?$"},
                severity="error"
            ),
            ValidationRule(
                name="valid_state_format",
                description="State must be valid two-letter code",
                field="location_state",
                rule_type="format",
                parameters={"valid_values": ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
                                           "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                                           "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                                           "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                                           "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]},
                severity="error"
            ),

            # Range validation rules
            ValidationRule(
                name="reasonable_receipts",
                description="Total receipts should be reasonable for a restaurant",
                field="total_receipts",
                rule_type="range",
                parameters={"min": 0, "max": 10000000},  # Max $10M annual
                severity="warning"
            ),
            ValidationRule(
                name="valid_latitude",
                description="Latitude must be valid",
                field="latitude",
                rule_type="range",
                parameters={"min": -90, "max": 90},
                severity="error"
            ),
            ValidationRule(
                name="valid_longitude",
                description="Longitude must be valid",
                field="longitude",
                rule_type="range",
                parameters={"min": -180, "max": 180},
                severity="error"
            ),

            # Consistency rules
            ValidationRule(
                name="address_city_consistency",
                description="City should be consistent in address",
                field="location_address",
                rule_type="consistency",
                parameters={"reference_field": "location_city"},
                severity="warning"
            ),

            # Concept classification rules
            ValidationRule(
                name="concept_confidence_threshold",
                description="Concept classification confidence should meet threshold",
                field="concept_confidence",
                rule_type="range",
                parameters={"min": 0.3},
                severity="warning"
            ),

            # Population data rules
            ValidationRule(
                name="reasonable_population",
                description="Population within 1 mile should be reasonable",
                field="population_1_mile",
                rule_type="range",
                parameters={"min": 0, "max": 100000},
                severity="warning"
            ),

            # Square footage rules
            ValidationRule(
                name="reasonable_square_footage",
                description="Square footage should be reasonable for restaurant",
                field="square_footage",
                rule_type="range",
                parameters={"min": 100, "max": 50000},
                severity="warning"
            )
        ]

    def add_validation_rule(self, rule: ValidationRule):
        """Add a custom validation rule"""
        self.validation_rules.append(rule)
        logger.info(f"Added validation rule: {rule.name}")

    def validate_record(self, record: Dict[str, Any], record_id: Optional[str] = None) -> List[ValidationResult]:
        """
        Validate a single record against all rules

        Args:
            record: Dictionary containing record data
            record_id: Optional record identifier

        Returns:
            List of ValidationResult objects
        """
        results = []

        for rule in self.validation_rules:
            if not rule.enabled:
                continue

            try:
                result = self._apply_rule(rule, record, record_id)
                if result:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error applying rule {rule.name}: {e}")
                results.append(ValidationResult(
                    rule_name=rule.name,
                    field=rule.field,
                    is_valid=False,
                    severity="error",
                    message=f"Rule application error: {e}",
                    record_id=record_id
                ))

        return results

    def _apply_rule(self, rule: ValidationRule, record: Dict[str, Any], record_id: Optional[str]) -> Optional[ValidationResult]:
        """Apply a single validation rule to a record"""
        field_value = record.get(rule.field)

        # Skip if field is missing and not required
        if field_value is None or field_value == '':
            if rule.rule_type == "completeness" and rule.parameters.get("required", False):
                return ValidationResult(
                    rule_name=rule.name,
                    field=rule.field,
                    is_valid=False,
                    severity=rule.severity,
                    message=rule.description,
                    actual_value=field_value,
                    record_id=record_id
                )
            else:
                return None  # Skip optional fields that are missing

        # Apply rule based on type
        if rule.rule_type == "format":
            return self._validate_format(rule, field_value, record_id)
        elif rule.rule_type == "range":
            return self._validate_range(rule, field_value, record_id)
        elif rule.rule_type == "completeness":
            return self._validate_completeness(rule, field_value, record_id)
        elif rule.rule_type == "consistency":
            return self._validate_consistency(rule, record, record_id)
        elif rule.rule_type == "custom":
            return self._validate_custom(rule, field_value, record, record_id)

        return None

    def _validate_format(self, rule: ValidationRule, value: Any, record_id: Optional[str]) -> Optional[ValidationResult]:
        """Validate field format"""
        if "pattern" in rule.parameters:
            pattern = rule.parameters["pattern"]
            # Use pre-compiled pattern if available
            if pattern == r"^\d{5}(-\d{4})?$":
                compiled_pattern = _ZIP_PATTERN
            else:
                compiled_pattern = re.compile(pattern)
            
            if not compiled_pattern.match(str(value)):
                return ValidationResult(
                    rule_name=rule.name,
                    field=rule.field,
                    is_valid=False,
                    severity=rule.severity,
                    message=f"Format validation failed for pattern: {pattern}",
                    actual_value=value,
                    record_id=record_id
                )

        if "valid_values" in rule.parameters:
            valid_values = rule.parameters["valid_values"]
            # Use frozenset for O(1) lookup instead of list comprehension
            if rule.field == "location_state":
                valid_set = _VALID_STATES_SET
            else:
                valid_set = frozenset(str(v).upper() for v in valid_values)
            
            if str(value).upper() not in valid_set:
                return ValidationResult(
                    rule_name=rule.name,
                    field=rule.field,
                    is_valid=False,
                    severity=rule.severity,
                    message=f"Value not in valid set: {valid_values}",
                    actual_value=value,
                    expected_value=valid_values,
                    record_id=record_id
                )

        return None

    def _validate_range(self, rule: ValidationRule, value: Any, record_id: Optional[str]) -> Optional[ValidationResult]:
        """Validate field range"""
        try:
            numeric_value = float(value)

            if "min" in rule.parameters and numeric_value < rule.parameters["min"]:
                return ValidationResult(
                    rule_name=rule.name,
                    field=rule.field,
                    is_valid=False,
                    severity=rule.severity,
                    message=f"Value below minimum: {rule.parameters['min']}",
                    actual_value=value,
                    expected_value=f">={rule.parameters['min']}",
                    record_id=record_id
                )

            if "max" in rule.parameters and numeric_value > rule.parameters["max"]:
                return ValidationResult(
                    rule_name=rule.name,
                    field=rule.field,
                    is_valid=False,
                    severity=rule.severity,
                    message=f"Value above maximum: {rule.parameters['max']}",
                    actual_value=value,
                    expected_value=f"<={rule.parameters['max']}",
                    record_id=record_id
                )

        except (ValueError, TypeError):
            return ValidationResult(
                rule_name=rule.name,
                field=rule.field,
                is_valid=False,
                severity=rule.severity,
                message="Value is not numeric",
                actual_value=value,
                record_id=record_id
            )

        return None

    def _validate_completeness(self, rule: ValidationRule, value: Any, record_id: Optional[str]) -> Optional[ValidationResult]:
        """Validate field completeness"""
        if rule.parameters.get("required", False) and (value is None or value == ''):
            return ValidationResult(
                rule_name=rule.name,
                field=rule.field,
                is_valid=False,
                severity=rule.severity,
                message="Required field is missing or empty",
                actual_value=value,
                record_id=record_id
            )

        return None

    def _validate_consistency(self, rule: ValidationRule, record: Dict[str, Any], record_id: Optional[str]) -> Optional[ValidationResult]:
        """Validate field consistency with other fields"""
        if "reference_field" in rule.parameters:
            ref_field = rule.parameters["reference_field"]
            ref_value = record.get(ref_field)

            if ref_value and ref_field in record.get(rule.field, ''):
                # Check if reference field value appears in main field
                if str(ref_value).lower() not in str(record.get(rule.field, '')).lower():
                    return ValidationResult(
                        rule_name=rule.name,
                        field=rule.field,
                        is_valid=False,
                        severity=rule.severity,
                        message=f"Field should contain reference value from {ref_field}",
                        actual_value=record.get(rule.field),
                        expected_value=ref_value,
                        record_id=record_id
                    )

        return None

    def _validate_custom(self, rule: ValidationRule, value: Any, record: Dict[str, Any], record_id: Optional[str]) -> Optional[ValidationResult]:
        """Apply custom validation logic"""
        # Placeholder for custom validation functions
        # Could be extended with user-defined validation functions
        return None

class DataQualityAnalyzer:
    """Advanced data quality analysis and anomaly detection"""

    def __init__(self):
        self.validation_engine = ValidationEngine()

    def analyze_dataset_quality(self, df: pd.DataFrame) -> QualityReport:
        """
        Perform comprehensive quality analysis on a dataset

        Args:
            df: DataFrame to analyze

        Returns:
            QualityReport with comprehensive analysis
        """
        logger.info(f"Starting quality analysis for dataset with {len(df)} records")

        all_results = []
        total_records = len(df)

        # Validate each record
        for idx, (_, record) in enumerate(df.iterrows()):
            record_id = str(record.get('id', f'record_{idx}'))
            record_dict = record.to_dict()

            results = self.validation_engine.validate_record(record_dict, record_id)
            all_results.extend(results)

        # Calculate quality scores
        quality_score = self._calculate_overall_quality_score(all_results, total_records)
        completeness_score = self._calculate_completeness_score(df)
        accuracy_score = self._calculate_accuracy_score(all_results)
        consistency_score = self._calculate_consistency_score(df)
        timeliness_score = self._calculate_timeliness_score(df)

        # Analyze issues using defaultdict for cleaner code
        errors_by_field = defaultdict(int)
        warnings_by_field = defaultdict(int)
        errors_by_type = defaultdict(int)

        for result in all_results:
            if not result.is_valid:
                # Count by field
                if result.severity == 'error':
                    errors_by_field[result.field] += 1
                else:
                    warnings_by_field[result.field] += 1

                # Count by type
                error_type = f"{result.severity}_{result.rule_name}"
                errors_by_type[error_type] += 1
        
        # Convert back to regular dicts for consistency
        errors_by_field = dict(errors_by_field)
        warnings_by_field = dict(warnings_by_field)
        errors_by_type = dict(errors_by_type)

        # Detect outliers and duplicates
        outlier_records = self._detect_outliers(df)
        duplicate_records = self._detect_duplicates(df)
        missing_data_patterns = self._analyze_missing_data_patterns(df)

        return QualityReport(
            total_records=total_records,
            validation_results=all_results,
            quality_score=quality_score,
            completeness_score=completeness_score,
            accuracy_score=accuracy_score,
            consistency_score=consistency_score,
            timeliness_score=timeliness_score,
            errors_by_field=errors_by_field,
            warnings_by_field=warnings_by_field,
            errors_by_type=errors_by_type,
            outlier_records=outlier_records,
            duplicate_records=duplicate_records,
            missing_data_patterns=missing_data_patterns
        )

    def _calculate_overall_quality_score(self, validation_results: List[ValidationResult], total_records: int) -> float:
        """Calculate overall quality score"""
        if total_records == 0:
            return 1.0

        # Weight errors and warnings differently
        error_count = sum(1 for r in validation_results if not r.is_valid and r.severity == 'error')
        warning_count = sum(1 for r in validation_results if not r.is_valid and r.severity == 'warning')

        # Calculate weighted score
        total_issues = error_count * 1.0 + warning_count * 0.5
        max_possible_issues = total_records * len(self.validation_engine.validation_rules)

        if max_possible_issues == 0:
            return 1.0

        quality_score = max(0.0, 1.0 - (total_issues / max_possible_issues))
        return round(quality_score, 3)

    def _calculate_completeness_score(self, df: pd.DataFrame) -> float:
        """Calculate data completeness score"""
        if df.empty:
            return 1.0

        # Define key fields that should be complete
        key_fields = [
            'location_name', 'location_address', 'location_city',
            'location_state', 'location_zip', 'total_receipts'
        ]

        completeness_scores = []

        for field in key_fields:
            if field in df.columns:
                completeness = 1.0 - (df[field].isnull().sum() / len(df))
                completeness_scores.append(completeness)

        return round(sum(completeness_scores) / len(completeness_scores), 3) if completeness_scores else 1.0

    def _calculate_accuracy_score(self, validation_results: List[ValidationResult]) -> float:
        """Calculate data accuracy score based on validation results"""
        if not validation_results:
            return 1.0

        valid_results = sum(1 for r in validation_results if r.is_valid)
        total_results = len(validation_results)

        return round(valid_results / total_results, 3) if total_results > 0 else 1.0

    def _calculate_consistency_score(self, df: pd.DataFrame) -> float:
        """Calculate data consistency score"""
        if df.empty:
            return 1.0

        consistency_scores = []

        # Check address consistency (city in address)
        if 'location_address' in df.columns and 'location_city' in df.columns:
            city_in_address = df.apply(
                lambda row: str(row.get('location_city', '')).lower() in str(row.get('location_address', '')).lower(),
                axis=1
            )
            consistency_scores.append(city_in_address.sum() / len(df))

        # Check state format consistency
        if 'location_state' in df.columns:
            valid_states = df['location_state'].astype(str).str.len() == 2
            consistency_scores.append(valid_states.sum() / len(df))

        return round(sum(consistency_scores) / len(consistency_scores), 3) if consistency_scores else 1.0

    def _calculate_timeliness_score(self, df: pd.DataFrame) -> float:
        """Calculate data timeliness score"""
        # For now, assume all data is current
        # In production, this would check data freshness against collection dates
        return 1.0

    def _detect_outliers(self, df: pd.DataFrame) -> List[str]:
        """Detect outlier records using statistical methods"""
        outlier_record_ids = []

        # Outlier detection for numeric fields
        numeric_fields = ['total_receipts', 'latitude', 'longitude', 'population_1_mile', 'square_footage']

        for field in numeric_fields:
            if field in df.columns:
                try:
                    values = pd.to_numeric(df[field], errors='coerce')
                    clean_values = values.dropna()

                    if len(clean_values) > 10:  # Need sufficient data for outlier detection
                        Q1 = clean_values.quantile(0.25)
                        Q3 = clean_values.quantile(0.75)
                        IQR = Q3 - Q1

                        lower_bound = Q1 - 1.5 * IQR
                        upper_bound = Q3 + 1.5 * IQR

                        outliers = df[(values < lower_bound) | (values > upper_bound)]
                        outlier_record_ids.extend(outliers['id'].astype(str).tolist())

                except Exception as e:
                    logger.warning(f"Error detecting outliers for field {field}: {e}")

        return list(set(outlier_record_ids))  # Remove duplicates

    def _detect_duplicates(self, df: pd.DataFrame) -> List[str]:
        """Detect duplicate records"""
        duplicate_ids = []

        try:
            # Check for exact duplicates
            duplicates = df[df.duplicated(subset=['location_name', 'location_address', 'location_city'], keep=False)]
            duplicate_ids.extend(duplicates['id'].astype(str).tolist())

            # Check for similar names/addresses (fuzzy matching would be better here)
            # For now, just flag exact duplicates

        except Exception as e:
            logger.warning(f"Error detecting duplicates: {e}")

        return list(set(duplicate_ids))

    def _analyze_missing_data_patterns(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Analyze patterns in missing data"""
        patterns = {}

        try:
            # Group by missing field combinations
            missing_mask = df.isnull()

            # Find records missing multiple fields
            for idx, (_, row) in enumerate(missing_mask.iterrows()):
                missing_fields = [col for col, missing in row.items() if missing]
                if len(missing_fields) > 1:
                    pattern_key = "_".join(sorted([str(field) for field in missing_fields]))
                    if pattern_key not in patterns:
                        patterns[pattern_key] = []
                    patterns[pattern_key].append(str(df.iloc[idx]['id']))

        except Exception as e:
            logger.warning(f"Error analyzing missing data patterns: {e}")

        return patterns

class DataCleaner:
    """Automated data cleaning and standardization"""

    def __init__(self):
        self.cleaning_rules = {}
        self._initialize_cleaning_rules()

    def _initialize_cleaning_rules(self):
        """Initialize data cleaning rules"""
        self.cleaning_rules = {
            'location_name': {
                'strip_whitespace': True,
                'title_case': True,
                'remove_extra_spaces': True
            },
            'location_address': {
                'strip_whitespace': True,
                'title_case': True,
                'standardize_street_suffix': True
            },
            'location_city': {
                'strip_whitespace': True,
                'title_case': True
            },
            'location_state': {
                'uppercase': True,
                'strip_whitespace': True
            },
            'location_zip': {
                'strip_whitespace': True,
                'format_zip': True
            }
        }

    def clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and standardize a single record

        Args:
            record: Dictionary containing record data

        Returns:
            Cleaned record dictionary
        """
        cleaned_record = record.copy()

        for field, rules in self.cleaning_rules.items():
            if field in cleaned_record:
                value = cleaned_record[field]

                if value is None or value == '':
                    continue

                original_value = value

                # Apply cleaning rules
                if rules.get('strip_whitespace', False):
                    value = str(value).strip()

                if rules.get('title_case', False):
                    value = str(value).title()

                if rules.get('uppercase', False):
                    value = str(value).upper()

                if rules.get('remove_extra_spaces', False):
                    value = re.sub(r'\s+', ' ', str(value))

                if rules.get('standardize_street_suffix', False):
                    value = self._standardize_street_suffix(str(value))

                if rules.get('format_zip', False):
                    value = self._format_zip_code(str(value))

                cleaned_record[field] = value

        return cleaned_record

    def _standardize_street_suffix(self, address: str) -> str:
        """Standardize street name suffixes"""
        suffix_map = {
            'st': 'Street',
            'st.': 'Street',
            'street': 'Street',
            'ave': 'Avenue',
            'ave.': 'Avenue',
            'avenue': 'Avenue',
            'ave': 'Avenue',
            'rd': 'Road',
            'rd.': 'Road',
            'road': 'Road',
            'blvd': 'Boulevard',
            'blvd.': 'Boulevard',
            'boulevard': 'Boulevard',
            'ln': 'Lane',
            'ln.': 'Lane',
            'lane': 'Lane',
            'dr': 'Drive',
            'dr.': 'Drive',
            'drive': 'Drive',
            'ct': 'Court',
            'ct.': 'Court',
            'court': 'Court',
            'pl': 'Place',
            'pl.': 'Place',
            'place': 'Place',
            'way': 'Way',
            'cir': 'Circle',
            'cir.': 'Circle',
            'circle': 'Circle'
        }

        # Replace common abbreviations with full words
        for abbr, full in suffix_map.items():
            pattern = r'\b' + re.escape(abbr) + r'\b'
            address = re.sub(pattern, full, address, flags=re.IGNORECASE)

        return address

    def _format_zip_code(self, zip_code: str) -> str:
        """Format ZIP code to standard format"""
        # Remove non-digits
        digits = re.sub(r'\D', '', zip_code)

        if len(digits) == 9:
            return f"{digits[:5]}-{digits[5:]}"
        elif len(digits) == 5:
            return digits
        else:
            return zip_code  # Return original if format is unexpected

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize a DataFrame

        Args:
            df: DataFrame to clean

        Returns:
            Cleaned DataFrame
        """
        logger.info(f"Starting data cleaning for DataFrame with {len(df)} records")

        cleaned_df = df.copy()

        # Apply cleaning to each record
        for idx, (_, record) in enumerate(cleaned_df.iterrows()):
            cleaned_record = self.clean_record(record.to_dict())
            for key, value in cleaned_record.items():
                cleaned_df.at[idx, key] = value

        logger.info("Data cleaning completed")
        return cleaned_df

class ValidationReporter:
    """Generate comprehensive validation and quality reports"""

    def __init__(self):
        self.analyzer = DataQualityAnalyzer()
        self.cleaner = DataCleaner()

    def generate_comprehensive_report(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate a comprehensive data quality report

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary containing complete report
        """
        logger.info("Generating comprehensive data quality report")

        # Clean data first
        cleaned_df = self.cleaner.clean_dataframe(df)

        # Analyze quality
        quality_report = self.analyzer.analyze_dataset_quality(cleaned_df)

        # Generate summary
        summary = {
            'report_generated_at': quality_report.generated_at.isoformat(),
            'dataset_overview': {
                'total_records': quality_report.total_records,
                'total_fields': len(df.columns) if not df.empty else 0,
                'overall_quality_score': quality_report.quality_score,
                'completeness_score': quality_report.completeness_score,
                'accuracy_score': quality_report.accuracy_score,
                'consistency_score': quality_report.consistency_score,
                'timeliness_score': quality_report.timeliness_score
            },
            'issue_summary': {
                'total_validation_errors': len([r for r in quality_report.validation_results if not r.is_valid and r.severity == 'error']),
                'total_validation_warnings': len([r for r in quality_report.validation_results if not r.is_valid and r.severity == 'warning']),
                'outlier_records_count': len(quality_report.outlier_records),
                'duplicate_records_count': len(quality_report.duplicate_records),
                'missing_data_patterns_count': len(quality_report.missing_data_patterns)
            },
            'top_issues': self._get_top_issues(quality_report),
            'recommendations': self._generate_recommendations(quality_report),
            'detailed_results': {
                'validation_results': [
                    {
                        'rule_name': r.rule_name,
                        'field': r.field,
                        'is_valid': r.is_valid,
                        'severity': r.severity,
                        'message': r.message,
                        'record_id': r.record_id
                    }
                    for r in quality_report.validation_results
                ],
                'errors_by_field': quality_report.errors_by_field,
                'warnings_by_field': quality_report.warnings_by_field,
                'outlier_records': quality_report.outlier_records,
                'duplicate_records': quality_report.duplicate_records
            }
        }

        return summary

    def _get_top_issues(self, quality_report: QualityReport) -> List[Dict[str, Any]]:
        """Get the most common issues"""
        issues = []

        # Add field error counts
        for field, count in quality_report.errors_by_field.items():
            issues.append({
                'type': 'validation_error',
                'field': field,
                'count': count,
                'description': f"{count} validation errors in {field}"
            })

        # Add outlier information
        if quality_report.outlier_records:
            issues.append({
                'type': 'outliers',
                'field': 'multiple',
                'count': len(quality_report.outlier_records),
                'description': f"{len(quality_report.outlier_records)} records with outlier values"
            })

        # Add duplicate information
        if quality_report.duplicate_records:
            issues.append({
                'type': 'duplicates',
                'field': 'multiple',
                'count': len(quality_report.duplicate_records),
                'description': f"{len(quality_report.duplicate_records)} potential duplicate records"
            })

        # Sort by count (descending)
        issues.sort(key=lambda x: x['count'], reverse=True)

        return issues[:10]  # Top 10 issues

    def _generate_recommendations(self, quality_report: QualityReport) -> List[str]:
        """Generate improvement recommendations"""
        recommendations = []

        # Quality score recommendations
        if quality_report.quality_score < 0.7:
            recommendations.append("Overall data quality is low. Consider reviewing data collection processes.")

        if quality_report.completeness_score < 0.8:
            recommendations.append("Data completeness is below 80%. Focus on collecting missing key fields.")

        if quality_report.accuracy_score < 0.9:
            recommendations.append("Data accuracy issues detected. Review validation rules and data sources.")

        # Specific field recommendations
        for field, error_count in quality_report.errors_by_field.items():
            if error_count > quality_report.total_records * 0.1:  # More than 10% error rate
                recommendations.append(f"High error rate in field '{field}'. Consider data cleansing or source review.")

        # Outlier recommendations
        if len(quality_report.outlier_records) > quality_report.total_records * 0.05:  # More than 5% outliers
            recommendations.append("High number of outlier records detected. Consider outlier removal or investigation.")

        # Missing data recommendations
        if len(quality_report.missing_data_patterns) > 0:
            recommendations.append("Missing data patterns detected. Consider improving data collection for affected fields.")

        if not recommendations:
            recommendations.append("Data quality is good. Continue current data collection and validation processes.")

        return recommendations

    def export_report_to_json(self, report: Dict[str, Any], filepath: str) -> str:
        """Export quality report to JSON file"""
        try:
            with open(filepath, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Quality report exported to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error exporting report: {e}")
            raise

    def print_report_summary(self, report: Dict[str, Any]):
        """Print a human-readable summary of the quality report"""
        print("\n" + "="*60)
        print("DATA QUALITY REPORT SUMMARY")
        print("="*60)

        overview = report['dataset_overview']
        issues = report['issue_summary']

        print("📊 Dataset Overview:")
        print(f"   • Total Records: {overview['total_records']:,}")
        print(f"   • Overall Quality Score: {overview['overall_quality_score']:.2%}")
        print(f"   • Completeness Score: {overview['completeness_score']:.2%}")
        print(f"   • Accuracy Score: {overview['accuracy_score']:.2%}")
        print(f"   • Consistency Score: {overview['consistency_score']:.2%}")

        print("\n🚨 Issues Found:")
        print(f"   • Validation Errors: {issues['total_validation_errors']}")
        print(f"   • Validation Warnings: {issues['total_validation_warnings']}")
        print(f"   • Outlier Records: {issues['outlier_records_count']}")
        print(f"   • Duplicate Records: {issues['duplicate_records_count']}")

        if report['top_issues']:
            print("\n🔍 Top Issues:")
            for issue in report['top_issues'][:5]:  # Show top 5
                print(f"   • {issue['description']} ({issue['count']} occurrences)")

        if report['recommendations']:
            print("\n💡 Recommendations:")
            for rec in report['recommendations'][:3]:  # Show top 3
                print(f"   • {rec}")

        print("="*60)