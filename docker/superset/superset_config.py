"""
Superset Configuration File for NoSQL Data Warehouse Project
"""
import os
from cachelib.file import FileSystemCache

# Database configuration
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# Security configuration
SECRET_KEY = 'your-secret-key-change-this-in-production'

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'FileSystemCache',
    'CACHE_DIR': '/app/superset_home/cache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_'
}

# Feature flags
FEATURE_FLAGS = {
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS_SET': True,
    'ENABLE_TEMPLATE_PROCESSING': True,
    'ALERT_REPORTS': True,
}

# Upload configuration
UPLOAD_FOLDER = '/app/superset_home/uploads/'
IMG_UPLOAD_FOLDER = '/app/superset_home/uploads/'
IMG_UPLOAD_URL = '/static/uploads/'

# Email configuration (optional)
SMTP_HOST = 'localhost'
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = 'admin@superset.com'
SMTP_PORT = 587
SMTP_PASSWORD = 'password'
SMTP_MAIL_FROM = 'admin@superset.com'

# WebDriver configuration for alerts and reports
WEBDRIVER_BASEURL = 'http://superset:8088/'
WEBDRIVER_BASEURL_USER_FRIENDLY = 'http://localhost:8088/'

# Row limit for SQL queries
ROW_LIMIT = 5000
VIZ_ROW_LIMIT = 10000

# CSV export configuration
CSV_EXPORT = {
    'encoding': 'utf-8',
}

# Time zone
DEFAULT_TIME_ZONE = 'UTC'

# Logging configuration
ENABLE_TIME_ROTATE = True
TIME_ROTATE_LOG_LEVEL = 'INFO'
FILENAME = '/app/superset_home/superset.log'

# Custom CSS
CUSTOM_CSS = """
.navbar-brand {
    font-weight: bold;
}
"""

# Database connections that will be added automatically
DATABASES_CONFIG = [
    {
        'database_name': 'Redshift Data Warehouse',
        'sqlalchemy_uri': 'redshift+psycopg2://admin:password@your-cluster.redshift.amazonaws.com:5439/dev',
        'allow_run_async': True,
        'allow_ctas': True,
        'allow_cvas': True,
        'allow_dml': False,
    },
    {
        'database_name': 'Cassandra (via Spark)',
        'sqlalchemy_uri': 'spark://spark-master:7077',
        'allow_run_async': True,
        'allow_ctas': False,
        'allow_cvas': False,
        'allow_dml': False,
    }
] 