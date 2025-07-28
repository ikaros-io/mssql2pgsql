# SQL Server to PostgreSQL Migration Tool

A robust, intelligent migration tool for transferring data from SQL Server to PostgreSQL with advanced features like resume functionality, batch optimization, and geo-spatial data handling.

## ✨ Features

- **🔄 Resume Capability**: Automatically resumes from where it left off if migration fails
- **🧠 Intelligent Batching**: Learns optimal batch sizes for each table and adapts over time
- **🗺️ Geo-Spatial Support**: Handles SQL Server geography/geometry types by converting to WKT format
- **🛡️ Data Validation**: Comprehensive type conversion and validation with graceful fallbacks
- **📊 Progress Tracking**: Real-time progress monitoring with detailed logging
- **⚡ Multi-Threading**: Concurrent table processing for improved performance  
- **🔧 YAML Configuration**: Secure, flexible configuration management
- **💾 Persistent State**: Maintains migration state across runs using pickle files

## 🚀 Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd eat-sql-extraction

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Copy the example configuration and update with your credentials:

```bash
cp config.yml.example config.yml
```

Edit `config.yml` with your database credentials:

```yaml
# SQL Server (Source Database)
sql_server:
  server: 'your-sql-server-host'
  database: 'YourDatabase'
  username: 'your-username'
  password: 'your-password'
  schema: 'dbo'

# PostgreSQL (Target Database)
postgresql:
  host: 'localhost'
  database: 'your_target_database'
  username: 'your-pg-username'
  password: 'your-pg-password'
  schema: 'your_target_schema'
```

### 3. Run Migration

```bash
# Basic migration with default settings
python migrate_db.py

# Custom configuration file
python migrate_db.py --config production.yml

# Override thread count
python migrate_db.py --threads 5

# Skip log tables
python migrate_db.py --skip-log-tables
```

## 📋 Configuration Reference

### Database Settings

```yaml
sql_server:
  server: 'localhost'           # SQL Server hostname
  port: 1433                    # SQL Server port
  database: 'YourDB'            # Source database name
  username: 'user'              # SQL Server username
  password: 'pass'              # SQL Server password
  driver: 'ODBC Driver 18 for SQL Server'
  schema: 'dbo'                 # Source schema
  
  connection:
    timeout: 300                # Connection timeout (seconds)
    trust_server_certificate: true
    login_timeout: 30
    connect_retry_count: 3
    connect_retry_interval: 10

postgresql:
  host: 'localhost'             # PostgreSQL hostname
  port: 5432                    # PostgreSQL port  
  database: 'target_db'         # Target database name
  username: 'postgres'          # PostgreSQL username
  password: 'password'          # PostgreSQL password
  schema: 'migrated_data'       # Target schema
```

### Migration Settings

```yaml
migration:
  default_chunk_size: 10000             # Default rows per batch
  default_sub_batch_size: 1000          # Default sub-batch size
  progress_save_interval: 10000         # Save progress every N rows
  preserve_temp_table_threshold: 1000   # Keep temp tables after N rows
  max_threads: 3                        # Concurrent table processing
  max_retries: 3                        # Retry attempts for failed operations
  retry_delay: 5                        # Delay between retries (seconds)
```

### Table-Specific Overrides

```yaml
table_overrides:
  LargeTable:
    chunk_size: 5000
    sub_batch_size: 400
    notes: "Large table with geography columns"
  
  ComplexTable:
    chunk_size: 2000
    sub_batch_size: 200
    notes: "Complex table with many joins"
```

## 🔧 Advanced Features

### Resume Functionality

If a migration fails partway through, the tool automatically preserves progress:

```
Found partial migration for Order: can resume from row 1,276,453
Previous progress: 1,276,453/1,636,478 (78.0%)
Resuming migration from offset 1,276,453 (skipping 78.0% already completed)
```

### Intelligent Batch Learning

The tool learns optimal batch sizes for each table and saves this information:

- **`batch_config.pkl`**: Binary configuration file for fast loading
- **`batch_config.json`**: Human-readable configuration backup

Example learned configuration:
```json
{
  "Order": {
    "optimal_chunk_size": 5000,
    "optimal_sub_batch_size": 400,
    "success_rate": 0.95,
    "geo_columns": 1,
    "column_count": 73,
    "notes": "Large table with geography columns"
  }
}
```

### Geography/Geometry Handling

SQL Server geography and geometry columns are automatically converted:

```sql
-- SQL Server
SELECT DeliveryGeoPoint FROM Orders

-- Converted to PostgreSQL
SELECT DeliveryGeoPoint::TEXT FROM Orders  -- Contains WKT format like 'POINT(151.2093 -33.8688)'
```

## 📊 Monitoring & Logging

### Progress Tracking

```
16:43:27  [INFO] Worker-0_0: → Migrating Order to PostgreSQL: 1,636,478 rows in chunks of 5,000
16:43:27  [INFO] Worker-0_0: → → Source: dbo.Order
16:43:27  [INFO] Worker-0_0: → → Columns: 73
16:43:27  [INFO] Worker-0_0: → Using optimized batch config: chunk=5000, sub_batch=400
16:43:34  [INFO] Worker-0_0: → Progress: 25.1% (412,453/1,636,478) - 2,341 rows/sec - ETA: 523s
```

### Error Handling

```
16:43:40  [WARNING] Worker-0_0: Bulk insert failed for sub-batch 1, trying row-by-row: integer out of range
16:43:40  [INFO] Worker-0_0: Preserving temp table Order_temp_1753692207628 with 1,276,453 rows for resume
```

## 🔍 Troubleshooting

### Common Issues

1. **Integer out of range errors**
   - The tool automatically handles this by using PostgreSQL BIGINT types
   - Large values outside range are converted to strings

2. **Geography/Geometry conversion failures**
   - Uses `STAsText()` to convert to WKT format
   - Handles NULL values properly

3. **Connection timeouts**
   - Adjust `timeout` settings in configuration
   - Reduce `chunk_size` for large tables

4. **Memory issues**
   - Reduce `default_chunk_size` and `default_sub_batch_size`
   - Lower `max_threads` count

### File Structure

```
eat-sql-extraction/
├── migrate_db.py              # Main migration script
├── config.yml                 # Your database configuration (not in git)
├── config.yml.example         # Configuration template
├── batch_config.pkl           # Learned batch configurations
├── batch_config.json          # Human-readable batch config
├── requirements.txt           # Python dependencies
├── .gitignore                # Excludes sensitive files
├── README.md                 # This file
└── logs/                     # Application logs directory
```

## 🔒 Security

- Configuration files with credentials are excluded from git via `.gitignore`
- Use `config.yml.example` as a template for team members
- Store production credentials securely (environment variables, secret managers)

## 🤝 Contributing

1. Copy `config.yml.example` to `config.yml` and update with your test database credentials
2. Ensure your changes don't break existing functionality
3. Test with various table structures including geography columns
4. Update documentation for new features

## 📝 License

MIT License

Copyright (c) 2024 SQL Server to PostgreSQL Migration Tool

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## 🐛 Issues & Support

For issues, bug reports, or feature requests, please create an issue in the repository.

---

**⚠️ Important**: Never commit `config.yml` or other files containing credentials to version control!