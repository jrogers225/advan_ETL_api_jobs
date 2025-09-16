# Advanced ETL API Jobs

A Python ETL (Extract, Transform, Load) pipeline for processing media plan data from SQL Server and loading it into Google BigQuery. This project includes automated data synchronization, error handling, logging, and scheduling capabilities.

## 🚀 Features

- **Media Plan Data ETL**: Extract media plan data from SQL Server and upsert to BigQuery
- **Automated Scheduling**: Windows batch file for Task Scheduler integration
- **Data Validation**: Key column validation and duplicate handling
- **Comprehensive Logging**: Detailed execution logs with automatic pruning
- **Flexible Filtering**: Date-based filtering for incremental loads
- **Error Handling**: Robust error handling with proper exit codes
- **Dry Run Mode**: Test runs without making actual changes

## 📁 Project Structure

```
advan_ETL_api_jobs/
├── config.py                    # Configuration constants and settings
├── requirements.txt             # Python dependencies
├── run_media_plan_load.bat     # Windows batch file for scheduling
├── .env                        # Environment variables (not in repo)
├── database/
│   ├── __init__.py
│   ├── database.py             # SQL Server connection and queries
│   ├── bigquery_init.py        # BigQuery initialization
│   └── bq_data_loader.py       # BigQuery data loading utilities
├── load_media_plan/
│   ├── __init__.py
│   └── main.py                 # Main ETL script
└── logs/                       # Execution logs (auto-created)
```

## 🛠️ Setup

### Prerequisites

- Python 3.8+
- SQL Server with ODBC Driver 18
- Google Cloud BigQuery access
- Windows OS (for batch scheduling)

### 1. Clone Repository

```bash
git clone https://github.com/jrogers225/advan_ETL_api_jobs.git
cd advan_ETL_api_jobs
```

### 2. Create Virtual Environment

```bash
python -m venv .venv
.venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Environment Configuration

Create a `.env` file in the project root:

```env
# SQL Server Configuration
DB_SERVER=your_sql_server
DB_DATABASE=your_database
DB_USER=your_username
DB_PASSWORD=your_password

# Google Cloud Configuration
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-key.json
GCP_PROJECT_ID=your_gcp_project_id
BQ_DATASET=your_bigquery_dataset
```

### 5. Google Cloud Authentication

1. Create a service account in Google Cloud Console
2. Download the JSON key file
3. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable
4. Ensure the service account has BigQuery Data Editor permissions

## 🎯 Usage

### Command Line Execution

```bash
# Basic execution
python load_media_plan/main.py

# With date filtering
python load_media_plan/main.py --start-date 2025-01-01

# Dry run mode (no changes made)
python load_media_plan/main.py --dry-run --start-date 2025-01-01
```

### Batch File Execution

```cmd
# Run with default date (120 days ago)
run_media_plan_load.bat

# Run with specific start date
run_media_plan_load.bat 2025-01-01
```

### Parameters

- `--start-date`: Filter records with StartDate >= this date (YYYY-MM-DD format)
- `--dry-run`: Show what would be done without executing changes

## 📊 Data Flow

1. **Extract**: Query media plan data from SQL Server view `SWD_V_MEDIA_PLAN`
2. **Transform**: 
   - Convert data types for BigQuery compatibility
   - Handle null values and duplicates
   - Apply date filtering if specified
3. **Load**: Upsert data to BigQuery table using key columns:
   - `media_plan_id`
   - `estimate_id`
   - `order_number`
   - `order_line_number`

## 🔧 Configuration

### SQL Server Connection

The project uses Windows Authentication by default. Modify `database/database.py` for different authentication methods:

```python
# Current: Windows Authentication
engine = create_engine(
    f"mssql+pyodbc://{DB_SERVER}/{DB_DATABASE}?trusted_connection=yes&driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no",
    fast_executemany=True
)

# Alternative: SQL Server Authentication
engine = create_engine(
    f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver=ODBC+Driver+18+for+SQL+Server",
    fast_executemany=True
)
```

### BigQuery Tables

Configure table names in `config.py`:

```python
MEDIA_PLAN_DATA_TABLE = 'media_plan_data_stg'
```

## 📋 Logging

Logs are automatically created in the `logs/` directory with timestamps:
- Format: `media_plan_load_YYYYMMDD_HHMMSS.log`
- Automatic pruning: Keeps only the 10 most recent log files
- Includes execution details, errors, and performance metrics

## ⏰ Scheduling

### Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (daily, weekly, etc.)
4. Set action to run the batch file:
   ```
   Program: C:\path\to\your\project\run_media_plan_load.bat
   Arguments: 2025-01-01 (optional start date)
   ```

### Batch File Features

- Automatic date calculation (120 days ago if no parameter)
- Log file management with timestamps
- Proper exit code handling for Task Scheduler
- Environment cleanup

## 🚨 Error Handling

The application handles various error scenarios:

- **Database Connection Issues**: Proper error messages and exit codes
- **Data Validation Errors**: Detailed logging of problematic records
- **BigQuery Upload Failures**: Retry logic and error reporting
- **Missing Key Columns**: Validation before processing
- **Duplicate Records**: Automatic deduplication with logging

## 🧪 Testing

### Dry Run Mode

Test the pipeline without making changes:

```bash
python load_media_plan/main.py --dry-run --start-date 2025-01-01
```

### Data Validation

The script automatically validates:
- Key column data types and null values
- Duplicate key combinations
- BigQuery schema compatibility

## 📈 Monitoring

### Log Analysis

Monitor execution through log files:
- Successful runs: Exit code 0
- Failed runs: Non-zero exit codes with error details
- Performance metrics: Record counts and execution times

### BigQuery Monitoring

Check BigQuery console for:
- Table update timestamps
- Row counts after upserts
- Query performance metrics

## 🔍 Troubleshooting

### Common Issues

1. **Import Errors**: Ensure virtual environment is activated and dependencies installed
2. **Database Connection**: Verify SQL Server accessibility and credentials
3. **BigQuery Permissions**: Check service account permissions
4. **ODBC Driver**: Install SQL Server ODBC Driver 18

### Debug Mode

Enable detailed logging by modifying the logging level in `main.py`:

```python
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
```

## 📝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/new-feature`)
5. Create Pull Request

## 📄 License

This project is proprietary software. All rights reserved.

## 👥 Authors

- **jrogers225** - Initial development and maintenance

## 🆘 Support

For issues and questions:
1. Check the logs in the `logs/` directory
2. Review the troubleshooting section
3. Create an issue in the GitHub repository

---

**Last Updated**: September 2025
