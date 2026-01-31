import os


# ----------------------------
# API Configuration
# ----------------------------

DUMMYJSON_BASE_URL = "https://dummyjson.com/products"

API_PAGE_SIZE = 20
API_MAX_RETRIES = 3
API_BACKOFF_SECONDS = 2

# Optional API authentication (used if provided)
API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN")


# ----------------------------
# Pipeline Configuration
# ----------------------------

PIPELINE_NAME = "dummyjson_products_pipeline"
