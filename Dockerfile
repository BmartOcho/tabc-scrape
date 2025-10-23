# Use Python 3.9 image with GCC 10 to avoid compilation issues
FROM python:3.9-bullseye

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY setup.py README.md ./

# Install the package
RUN pip install -e .

# Create data directory
RUN mkdir -p data

# Set default environment
ENV ENVIRONMENT=prod

# Expose port for web server
EXPOSE 5000

# Default command
CMD ["tabc-scrape", "serve", "--host", "0.0.0.0", "--port", "5000"]