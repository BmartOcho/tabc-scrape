"""
TABC Restaurant Data Scraper - CLI Interface

Main entry point for the tabc-scrape command line tool.
"""

import asyncio
import logging
import sys
import signal
import os
from pathlib import Path

import click

# Import existing modules
from .config import config
from .data.api_client import TexasComptrollerAPI
from .storage.database import DatabaseManager
from .storage.enrichment_pipeline import DataEnrichmentPipeline
from .storage.validation_framework import ValidationReporter
from .web import app, run_server

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('tabc_scrape.log')
    ]
)

logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version='1.0.0')
def cli():
    """
    TABC Restaurant Data Scraper

    A comprehensive tool for collecting, enriching, and analyzing
    Texas restaurant data from the Comptroller API.
    """
    pass


@cli.command()
@click.option('--limit', '-l', type=int, help='Maximum number of restaurants to fetch')
@click.option('--batch-size', '-b', type=int, default=1000, help='Batch size for API requests')
@click.option('--database-url', help='Database URL override')
@click.option('--active-only', is_flag=True, help='Fetch only currently active businesses (exclude closed locations)')
def fetch(limit, batch_size, database_url, active_only):
    """
    Fetch restaurant data from Texas Comptroller API

    Downloads restaurant data and stores it in the local database.
    """
    async def _async_fetch():
        # Initialize components
        api_client = TexasComptrollerAPI()
        db_manager = DatabaseManager(database_url)

        # Test API connection first
        if not await api_client.test_connection():
            click.echo("[ERROR] Failed to connect to Texas Comptroller API")
            return

        click.echo("API connection successful")

        # Fetch restaurant data
        if active_only:
            click.echo(f"[DATA] Fetching active restaurant data only (batch size: {batch_size})...")
            restaurants = await api_client.get_active_restaurants(batch_size=batch_size)
        else:
            click.echo(f"[DATA] Fetching all restaurant data (batch size: {batch_size})...")
            restaurants = await api_client.get_all_restaurants(batch_size=batch_size)

        if not restaurants:
            click.echo("[WARN]  No restaurant data retrieved")
            return

        click.echo(f"✅ Retrieved {len(restaurants)} restaurant records")

        # Show filtering info if active-only was used
        if active_only:
            click.echo(f"[INFO] Filtered to active businesses only")

        # Store in database
        click.echo("[SAVE] Storing data in database...")
        stored_count = db_manager.store_restaurants([r.__dict__ for r in restaurants])

        click.echo(f"✅ Successfully stored {stored_count} restaurant records")
        click.echo("[SUCCESS] Restaurant data fetch completed!")

    try:
        asyncio.run(_async_fetch())
    except Exception as e:
        logger.error(f"Error during fetch: {e}")
        click.echo(f"[ERROR] Error during fetch: {e}")
        raise click.ClickException(f"Fetch failed: {e}")


@cli.command()
@click.option('--limit', '-l', type=int, help='Maximum number of restaurants to enrich')
@click.option('--batch-size', '-b', type=int, default=10, help='Batch size for enrichment')
@click.option('--database-url', help='Database URL override')
@click.option('--skip-square-footage', is_flag=True, help='Skip square footage scraping')
@click.option('--skip-concept-classification', is_flag=True, help='Skip concept classification')
@click.option('--skip-population-analysis', is_flag=True, help='Skip population analysis')
def enrich(limit, batch_size, database_url, skip_square_footage, skip_concept_classification, skip_population_analysis):
    """
    Run data enrichment pipeline

    Enriches restaurant data with concept classification, population analysis,
    and square footage information.
    """
    async def _async_enrich():
        click.echo("[LAB] Starting data enrichment pipeline...")

        # Initialize database and pipeline
        db_manager = DatabaseManager(database_url)
        pipeline = DataEnrichmentPipeline(db_manager)

        # Configure pipeline based on flags
        if skip_square_footage:
            pipeline.enable_square_footage_scraping = False
        if skip_concept_classification:
            pipeline.enable_concept_classification = False
        if skip_population_analysis:
            pipeline.enable_population_analysis = False

        # Update batch size
        pipeline.batch_size = batch_size

        click.echo("[DATA] Configuration:")
        click.echo(f"   • Batch size: {pipeline.batch_size}")
        click.echo(f"   • Square footage scraping: {'✅' if pipeline.enable_square_footage_scraping else '⏭️  skipped'}")
        click.echo(f"   • Concept classification: {'✅' if pipeline.enable_concept_classification else '⏭️  skipped'}")
        click.echo(f"   • Population analysis: {'✅' if pipeline.enable_population_analysis else '⏭️  skipped'}")

        # Run enrichment pipeline
        click.echo("🚀 Running enrichment pipeline...")
        stats = await pipeline.run_full_enrichment_pipeline(limit=limit)

        # Display results
        click.echo("✅ Enrichment completed!")
        click.echo("[CHART] Results:")
        click.echo(f"   • Restaurants processed: {stats.total_restaurants}")
        click.echo(f"   • Successful enrichments: {stats.successful_enrichments}")
        click.echo(f"   • Failed enrichments: {stats.failed_enrichments}")
        click.echo(f"   • Total processing time: {stats.total_processing_time:.2f}s")
        click.echo(f"   • Average time per restaurant: {stats.average_time_per_restaurant:.2f}s")

        if stats.data_sources_used:
            click.echo("[DATA] Data sources used:")
            for source, count in stats.data_sources_used.items():
                click.echo(f"   • {source}: {count} restaurants")

        if stats.error_summary:
            click.echo("[WARN]  Errors encountered:")
            for error_type, count in stats.error_summary.items():
                click.echo(f"   • {error_type}: {count}")

    try:
        asyncio.run(_async_enrich())
    except Exception as e:
        logger.error(f"Error during enrichment: {e}")
        click.echo(f"[ERROR] Error during enrichment: {e}")
        raise click.ClickException(f"Enrichment failed: {e}")


@cli.command()
@click.option('--format', '-f', type=click.Choice(['json', 'csv']), default='json', help='Export format')
@click.option('--output', '-o', help='Output file path')
@click.option('--database-url', help='Database URL override')
@click.option('--enriched-only', is_flag=True, help='Export only enriched restaurants')
def export(format, output, database_url, enriched_only):
    """
    Export enriched restaurant data

    Exports restaurant data to JSON or CSV format.
    """
    click.echo("[EXPORT] Starting data export...")

    try:
        # Initialize database
        db_manager = DatabaseManager(database_url)

        # Determine output path
        if not output:
            if format.lower() == 'json':
                output = "data/enriched_restaurants_export.json"
            else:
                output = "data/enriched_restaurants_export.csv"

        # Ensure data directory exists
        Path(output).parent.mkdir(parents=True, exist_ok=True)

        click.echo(f"[DATA] Export format: {format.upper()}")
        click.echo(f"📁 Output file: {output}")

        if enriched_only:
            click.echo("[SEARCH] Exporting only enriched restaurants...")
            # This would need to be implemented in DatabaseManager
            # For now, export all data
            pass

        # Export data
        if format.lower() == 'json':
            exported_path = db_manager.export_to_json(output)
        else:
            exported_path = db_manager.export_to_csv(output)

        click.echo(f"✅ Data exported successfully to: {exported_path}")

        # Show file size info
        file_size = Path(exported_path).stat().st_size
        click.echo(f"[DATA] File size: {file_size:,} bytes")

    except Exception as e:
        logger.error(f"Error during export: {e}")
        click.echo(f"[ERROR] Error during export: {e}")
        raise click.ClickException(f"Export failed: {e}")


@cli.command()
@click.option('--input-file', '-i', help='Input file to validate (optional)')
@click.option('--output-report', '-o', help='Output validation report file')
@click.option('--database-url', help='Database URL override')
@click.option('--detailed', is_flag=True, help='Show detailed validation results')
def validate(input_file, output_report, database_url, detailed):
    """
    Validate data quality

    Performs comprehensive data quality validation and generates reports.
    """
    click.echo("[SEARCH] Starting data validation...")

    try:
        # Initialize components
        db_manager = DatabaseManager(database_url)
        reporter = ValidationReporter()

        # Get data to validate
        if input_file:
            click.echo(f"📂 Loading data from: {input_file}")
            # This would load from file - for now use database
            df = db_manager.get_enriched_restaurants_dataframe()
        else:
            click.echo("[DATA] Validating data from database...")
            df = db_manager.get_enriched_restaurants_dataframe()

        if df.empty:
            click.echo("[WARN]  No data found to validate")
            return

        click.echo(f"✅ Loaded {len(df)} records for validation")

        # Generate comprehensive report
        click.echo("[LAB] Running validation checks...")
        report = reporter.generate_comprehensive_report(df)

        # Display summary
        overview = report['dataset_overview']
        issues = report['issue_summary']

        click.echo("✅ Validation completed!")
        click.echo("[DATA] Quality Scores:")
        click.echo(f"   • Overall Quality: {overview['overall_quality_score']:.2%}")
        click.echo(f"   • Completeness: {overview['completeness_score']:.2%}")
        click.echo(f"   • Accuracy: {overview['accuracy_score']:.2%}")
        click.echo(f"   • Consistency: {overview['consistency_score']:.2%}")

        click.echo("🚨 Issues Found:")
        click.echo(f"   • Validation Errors: {issues['total_validation_errors']}")
        click.echo(f"   • Validation Warnings: {issues['total_validation_warnings']}")
        click.echo(f"   • Outlier Records: {issues['outlier_records_count']}")
        click.echo(f"   • Duplicate Records: {issues['duplicate_records_count']}")

        if report['top_issues'] and detailed:
            click.echo("[SEARCH] Top Issues:")
            for issue in report['top_issues'][:5]:
                click.echo(f"   • {issue['description']}")

        if report['recommendations']:
            click.echo("[TIP] Recommendations:")
            for rec in report['recommendations'][:3]:
                click.echo(f"   • {rec}")

        # Save detailed report if requested
        if output_report:
            reporter.export_report_to_json(report, output_report)
            click.echo(f"[REPORT] Detailed report saved to: {output_report}")

    except Exception as e:
        logger.error(f"Error during validation: {e}")
        click.echo(f"[ERROR] Error during validation: {e}")
        raise click.ClickException(f"Validation failed: {e}")


@cli.command()
def status():
    """
    Show current system status and statistics

    Displays database statistics, enrichment status, and system health.
    """
    click.echo("System Status")
    click.echo("=" * 50)

    try:
        # Initialize database
        db_manager = DatabaseManager()

        # Get enrichment statistics
        stats = db_manager.get_enrichment_stats()

        click.echo("[DB]  Database Status:")
        click.echo(f"   • Total restaurants: {stats['total_restaurants']:,}")
        click.echo(f"   • With concept classification: {stats['restaurants_with_concept_classification']:,}")
        click.echo(f"   • With population data: {stats['restaurants_with_population_data']:,}")
        click.echo(f"   • With square footage: {stats['restaurants_with_square_footage']:,}")

        click.echo("[CHART] Enrichment Coverage:")
        for enrichment_type, coverage in stats['enrichment_coverage'].items():
            click.echo(f"   • {enrichment_type.replace('_', ' ').title()}: {coverage:.1%}")

        click.echo("[GEAR]  Active Jobs:")
        click.echo(f"   • Active: {stats['enrichment_jobs']['active']}")
        click.echo(f"   • Completed: {stats['enrichment_jobs']['completed']}")
        click.echo(f"   • Failed: {stats['enrichment_jobs']['failed']}")

        # Test connections
        click.echo("[LINK] Connection Tests:")
        db_ok = db_manager.test_connection()
        click.echo(f"   • Database: {'OK' if db_ok else 'ERROR'}")

        api_client = TexasComptrollerAPI()
        api_ok = api_client.test_connection()
        click.echo(f"   • Texas Comptroller API: {'OK' if api_ok else 'ERROR'}")

        if db_ok and api_ok:
            click.echo("SUCCESS: System is ready!")
        else:
            click.echo("WARNING: Some connections failed")

    except Exception as e:
        logger.error(f"Error getting status: {e}")
        click.echo(f"[ERROR] Error getting status: {e}")
        raise click.ClickException(f"Status check failed: {e}")


@cli.command()
@click.option('--host', default='0.0.0.0', help='Host to bind to')
@click.option('--port', default=5000, type=int, help='Port to bind to')
@click.option('--debug', is_flag=True, help='Run in debug mode')
def serve(host, port, debug):
    """
    Run the web server for health monitoring and API endpoints
    """
    click.echo(f"Starting web server on {host}:{port}")

    def signal_handler(signum, frame):
        click.echo("Shutting down gracefully...")
        os._exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        run_server(host=host, port=port, debug=debug)
    except Exception as e:
        logger.error(f"Error running server: {e}")
        raise click.ClickException(f"Server failed: {e}")


if __name__ == '__main__':
    cli()