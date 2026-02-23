Bronze Layer Documentation
1. Objective

The Bronze layer stores raw ingested data exactly as received from source systems.

This layer:

Preserves original schema

Does not apply business transformations

Supports traceability and auditability

Serves as source for Silver transformations

2. Ingestion Tool

Azure Data Factory (ADF)

Ingestion type: Batch (CSV files)

Trigger: Manual / Scheduled (define yours)

3. Source Details
Source Type	Format	Frequency	Storage
CSV Files	CSV	Daily batch	Local / Cloud Upload
4. Target Storage

Azure Data Lake Gen2

Container: bronze

Folder structure:

bronze/
  ├── dataset_1/
  │     └── load_date=YYYY-MM-DD/
  ├── dataset_2/
5. Data Handling Strategy

✔ No transformations
✔ No deduplication
✔ No null handling
✔ Schema preserved as-is

6. Metadata Captured

The following metadata fields are added during ingestion:

ingestion_timestamp

source_filename

load_date

7. Data Validation at Bronze

Basic validation performed:

File existence check

File format validation

Row count capture

Schema drift logging (if enabled in ADF)

8. Logging & Monitoring

ADF pipeline logs stored in Azure Monitor

Failed pipeline alerts configured
