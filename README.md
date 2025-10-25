# TABC Restaurant Data Scraper

A comprehensive CLI tool for collecting, enriching, and analyzing Texas restaurant data from the Comptroller API.

## Features

- **📊 Data Collection**: Fetch restaurant data from the Texas Comptroller API
- **🔬 Data Enrichment**: Enhance data with concept classification, population analysis, and square footage information
- **📤 Data Export**: Export enriched data to JSON or CSV formats
- **🔍 Data Validation**: Comprehensive data quality validation and reporting
- **📈 Status Monitoring**: Real-time system status and statistics

## Installation

### From Source (Recommended for Development)

```bash
# Clone the repository
git clone <repository-url>
cd tabc-scrape

# Install in development mode
pip install -e .

# Or install with all dependencies
pip install -r requirements.txt
```

### System-wide Installation

```bash
# Install the package system-wide
pip install .

# The CLI will be available as 'tabc-scrape' command
```

## Quick Start

### 1. Fetch Restaurant Data

```bash
# Fetch all restaurant data (be respectful with API limits)
tabc-scrape fetch

# Fetch with custom batch size
tabc-scrape fetch --batch-size 500

# Fetch limited number of records for testing
tabc-scrape fetch --limit 100
```

### 2. Enrich the Data

```bash
# Run full enrichment pipeline
tabc-scrape enrich

# Run with custom batch size
tabc-scrape enrich --batch-size 5

# Skip specific enrichment steps
tabc-scrape enrich --skip-square-footage --skip-population-analysis

# Process only a subset of restaurants
tabc-scrape enrich --limit 50
```

### 3. Export Enriched Data

```bash
# Export to JSON (default)
tabc-scrape export

# Export to CSV
tabc-scrape export --format csv

# Export to specific file
tabc-scrape export --output ./data/my_restaurants.json
```

### 4. Validate Data Quality

```bash
# Basic validation
tabc-scrape validate

# Detailed validation with top issues
tabc-scrape validate --detailed

# Save validation report
tabc-scrape validate --output-report ./reports/validation.json

# Validate specific file
tabc-scrape validate --input-file ./data/restaurants.json
```

### 5. Check System Status

```bash
# Show current status and statistics
tabc-scrape status
```

## Command Reference

### Global Options

- `--help` or `-h`: Show help for any command
- `--version`: Show version information

### `tabc-scrape fetch`

Fetch restaurant data from Texas Comptroller API.

**Options:**
- `-l, --limit INTEGER`: Maximum number of restaurants to fetch
- `-b, --batch-size INTEGER`: Batch size for API requests (default: 1000)
- `--database-url TEXT`: Database URL override

**Examples:**
```bash
tabc-scrape fetch --limit 1000 --batch-size 500
```

### `tabc-scrape enrich`

Run data enrichment pipeline.

**Options:**
- `-l, --limit INTEGER`: Maximum number of restaurants to enrich
- `-b, --batch-size INTEGER`: Batch size for enrichment (default: 10)
- `--database-url TEXT`: Database URL override
- `--skip-square-footage`: Skip square footage scraping
- `--skip-concept-classification`: Skip concept classification
- `--skip-population-analysis`: Skip population analysis

**Examples:**
```bash
tabc-scrape enrich --batch-size 5 --skip-square-footage
```

### `tabc-scrape export`

Export enriched restaurant data.

**Options:**
- `-f, --format [json|csv]`: Export format (default: json)
- `-o, --output TEXT`: Output file path
- `--database-url TEXT`: Database URL override
- `--enriched-only`: Export only enriched restaurants

**Examples:**
```bash
tabc-scrape export --format csv --output ./exports/restaurants.csv
```

### `tabc-scrape validate`

Validate data quality.

**Options:**
- `-i, --input-file TEXT`: Input file to validate (optional)
- `-o, --output-report TEXT`: Output validation report file
- `--database-url TEXT`: Database URL override
- `--detailed`: Show detailed validation results

**Examples:**
```bash
tabc-scrape validate --detailed --output-report ./reports/quality.json
```

### `tabc-scrape status`

Show current system status and statistics.

**Examples:**
```bash
tabc-scrape status
```

## Configuration

### Environment Variables

The application can be configured using environment variables:

- `TABC_API_URL`: Override the default Texas Comptroller API URL
- `TABC_DB_URL`: Override the default database URL
- `TABC_SCRAPING_DELAY`: Set delay between scraping requests (seconds)
- `CENSUS_API_KEY`: Optional Census API key for better rate limits
- `APP_TOKEN`: Optional Socrata App Token for API access
- `ENVIRONMENT`: Set to 'dev' or 'prod' (default: dev)

### Database Configuration

By default, the application uses SQLite (`sqlite:///tabc_restaurants.db`). You can override this by:

1. Setting the `TABC_DB_URL` environment variable
2. Using the `--database-url` option with commands

**Example:**
```bash
export TABC_DB_URL="postgresql://user:password@localhost/tabc_data"
tabc-scrape fetch
```

## Replit Deployment

This project can be deployed on Replit for easy web-based access to the data pipeline.

### Setup in Replit

1. **Import the Project**: Upload or clone this repository to Replit.

2. **Install Dependencies**: Replit will automatically install dependencies from `pyproject.toml` or `requirements.txt`.

3. **Environment Variables**: Set the following in Replit's Secrets tab:
   - `CENSUS_API_KEY`: Your Census API key (optional)
   - `APP_TOKEN`: Your Socrata App Token (optional)
   - `TABC_DB_URL`: Database URL (default: `sqlite:///tabc_restaurants.db`)
   - `ENVIRONMENT`: Set to `dev`

4. **Run the App**: The `.replit` file is configured to run `replit_app.py`, which starts the web server.

### Using the Web Interface

Once deployed, access the dashboard at your Replit app URL:
- **Dashboard**: View system status, statistics, and recent data
- **Fetch Data**: Trigger data collection from the API
- **Enrich Data**: Run the enrichment pipeline
- **Export Data**: Download enriched data as CSV
- **Full Pipeline**: Run the complete fetch and enrich process

### API Endpoints

The web server provides RESTful endpoints:
- `GET /`: Main dashboard
- `GET /status`: System status and statistics
- `GET /metrics`: Prometheus metrics
- `GET /api/enriched-data`: Get enriched data (JSON)
- `GET /api/enriched-data/csv`: Get enriched data (CSV)
- `POST /api/trigger/fetch`: Trigger data fetch
- `POST /api/trigger/enrich`: Trigger data enrichment
- `POST /api/workflow/full`: Run full pipeline

## Data Pipeline Overview

1. **Fetch**: Collect raw restaurant data from Texas Comptroller API
2. **Store**: Save raw data to local database
3. **Enrich**: Enhance data with:
   - Concept classification (restaurant type/category)
   - Population analysis (demographics within radius)
   - Square footage information (scraped from web)
4. **Validate**: Check data quality and generate reports
5. **Export**: Export enriched data for analysis

## Output Files

- **Database**: `tabc_restaurants.db` (SQLite by default)
- **Logs**: `tabc_scrape.log`
- **Exports**: `data/enriched_restaurants_export.json` (or CSV)
- **Reports**: `reports/validation.json` (if generated)

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src/tabc_scrape

# Run specific test file
pytest tests/test_api_client.py
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
flake8 src/ tests/

# Type checking (if mypy is installed)
mypy src/
```

### Project Structure

```
tabc-scrape/
├── src/
│   └── tabc_scrape/
│       ├── __init__.py
│       ├── cli.py              # Main CLI entry point
│       ├── config.py           # Configuration management
│       ├── data/
│       │   └── api_client.py   # Texas Comptroller API client
│       ├── scraping/
│       │   ├── concept_classifier.py
│       │   └── square_footage.py
│       ├── storage/
│       │   ├── database.py
│       │   ├── enrichment_pipeline.py
│       │   ├── models.py
│       │   └── validation_framework.py
│       └── utils/
├── tests/                      # Test files
├── data/                       # Data exports
├── reports/                    # Validation reports
├── requirements.txt
├── setup.py
└── README.md
```

## API Reference

### Core Classes

- `TexasComptrollerAPI`: Handles communication with Texas Comptroller API
- `DatabaseManager`: Manages database operations and data storage
- `DataEnrichmentPipeline`: Coordinates the enrichment process
- `ValidationReporter`: Generates data quality reports

### Data Models

- `RestaurantRecord`: Restaurant data structure
- `EnrichmentResult`: Results from enrichment operations
- `QualityReport`: Comprehensive data quality assessment

## Troubleshooting

### Common Issues

1. **API Rate Limiting**: The Texas Comptroller API has rate limits. Use the `--batch-size` and respect delays between requests.

2. **Database Connection Errors**: Ensure the database URL is correct and the database server is running.

3. **Missing Dependencies**: Install all requirements:
   ```bash
   pip install -r requirements.txt
   ```

4. **Scraping Issues**: Some websites may block automated requests. The scraper includes delays and user agent rotation.

### Logging

The application logs to both console and `tabc_scrape.log` file. Log levels can be controlled via the logging configuration in `cli.py`.

### Debug Mode

For more detailed logging, you can modify the logging level in the CLI module:

```python
logging.basicConfig(level=logging.DEBUG, ...)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues, questions, or contributions, please:

1. Check the existing issues on GitHub
2. Create a new issue with detailed information
3. Include error messages, configuration, and steps to reproduce

## Changelog

### Version 1.0.0

- Initial release
- Complete CLI interface with all required commands
- Data fetching from Texas Comptroller API
- Comprehensive enrichment pipeline
- Data validation and quality reporting
- Export functionality (JSON/CSV)
- System status monitoring