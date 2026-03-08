Source to Target Mapping Document
1. Purpose

This document defines the transformation logic from source data (Bronze layer) to curated data models (Silver/Gold layers).

It ensures:

Data lineage transparency

Business rule clarity

Standardized data definitions

Trust in analytics outputs

2. Mapping Structure

Each record transformation follows:

Source → Transformation Rule → Target

3. Example Mapping Table
Source Table	Source Column	Target Table	Target Column	Transformation Rule	Data Type	Notes
bronze_orders	order_id	fact_orders	order_key	Direct mapping	INT	Primary Key
bronze_orders	order_date	fact_orders	order_date_key	Convert to Date Key	INT	YYYYMMDD format
bronze_orders	customer_id	dim_customer	customer_key	Surrogate Key	INT	Generated in Silver
bronze_orders	amount	fact_orders	total_amount	Cast to DECIMAL(10,2)	DECIMAL	Null → 0
4. Transformation Types

Direct Mapping

Data Type Conversion

Null Handling

Standardization

Derived Columns

Surrogate Key Generation

Aggregation (if applicable)

5. Business Rule Documentation

Example:

If amount is NULL → set to 0

If order_status = 'C' → map to 'Completed'

Date format standardized to YYYY-MM-DD
