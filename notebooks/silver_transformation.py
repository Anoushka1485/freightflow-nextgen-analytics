"""
FreightFlow NextGen Analytics
Silver Transformation Layer — Bronze (CSV) → Silver (Parquet)

Applies all transformations defined in the STT Mapping document:
  - Column renaming (snake_case, unit suffixes)
  - Data type casting
  - Value standardization
  - Typo corrections
  - Data quality checks (nulls, duplicates, range validation)
  - Row count reconciliation (Bronze vs Silver)

Run locally:
    python silver_transformation.py

Run on Databricks:
    Upload to workspace and run as a job or notebook cell.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, DecimalType, BooleanType, DateType, TimestampType, StringType
)
from datetime import datetime
import os
import json

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("FreightFlow_Silver_Transformation") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Paths ─────────────────────────────────────────────────────────────────────
BRONZE_PATH = "data/bronze/"
SILVER_PATH = "data/silver/"
LOG_PATH    = "docs/data-lineage/silver_reconciliation_log.json"

os.makedirs(SILVER_PATH, exist_ok=True)
os.makedirs("docs/data-lineage", exist_ok=True)

# ── Reconciliation Log ────────────────────────────────────────────────────────
reconciliation = []

def log_reconciliation(table, bronze_count, silver_count):
    status = "PASS" if bronze_count == silver_count else "FAIL"
    entry = {
        "table":        table,
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "status":       status,
        "timestamp":    datetime.now().isoformat()
    }
    reconciliation.append(entry)
    flag = "✅" if status == "PASS" else "❌"
    print(f"  {flag} Reconciliation [{table}]: Bronze={bronze_count} | Silver={silver_count} | {status}")

def read_bronze(table_name):
    path = os.path.join(BRONZE_PATH, f"{table_name}.csv")
    df = spark.read.csv(path, header=True, inferSchema=True)
    print(f"\n📥 Reading Bronze: {table_name} ({df.count()} rows)")
    return df

def write_silver(df, table_name, bronze_count):
    path = os.path.join(SILVER_PATH, table_name)
    df.write.mode("overwrite").parquet(path)
    silver_count = df.count()
    print(f"  💾 Written to Silver: {path} ({silver_count} rows)")
    log_reconciliation(table_name, bronze_count, silver_count)
    return df

def standardize(col_name, mapping):
    """Build a CASE WHEN expression for value standardization."""
    expr = F.col(col_name)
    for old, new in mapping.items():
        expr = F.when(F.upper(F.col(col_name)) == old.upper(), new).otherwise(expr)
    return expr

# ─────────────────────────────────────────────────────────────────────────────
# 1. VEHICLE
# Source: Vehicle.csv
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Vehicle")
print("="*60)

df = read_bronze("vehicle")
bronze_count = df.count()

# DQ: check nulls on PK
null_pk = df.filter(F.col("Vehicle_ID").isNull()).count()
if null_pk > 0:
    print(f"  ⚠️  DQ WARNING: {null_pk} null Vehicle_ID rows — dropping")
    df = df.filter(F.col("Vehicle_ID").isNotNull())

df_vehicle = df.select(
    F.trim(F.upper(F.col("Vehicle_ID"))).alias("vehicle_id"),
    F.trim(F.col("Registration_Number")).alias("registration_number"),
    F.to_timestamp(F.col("Last_Service_Date")).alias("last_service_date"),
    F.to_timestamp(F.col("Next_Service_Date")).alias("next_service_date"),
    standardize("V_Status", {
        "Active":             "Active",
        "Dormant":            "Dormant",
        "Parked":             "Parked",
        "Undermaintainence":  "UnderMaintenance",
        "Undermaintenance":   "UnderMaintenance",
    }).alias("vehicle_status"),
    standardize("V_Type", {
        "Truck":   "Truck",
        "Trailer": "Trailer",
    }).alias("vehicle_type"),
)

write_silver(df_vehicle, "vehicle", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# 2. TRUCKS
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Trucks")
print("="*60)

df = read_bronze("trucks")
bronze_count = df.count()

null_pk = df.filter(F.col("Truck_ID").isNull()).count()
if null_pk > 0:
    print(f"  ⚠️  DQ WARNING: {null_pk} null Truck_ID rows — dropping")
    df = df.filter(F.col("Truck_ID").isNotNull())

# DQ: mileage >= 0
invalid_mileage = df.filter(F.col("Current_Mileage") < 0).count()
if invalid_mileage > 0:
    print(f"  ⚠️  DQ WARNING: {invalid_mileage} rows with negative mileage — setting to NULL")
    df = df.withColumn("Current_Mileage",
        F.when(F.col("Current_Mileage") < 0, None).otherwise(F.col("Current_Mileage")))

df_trucks = df.select(
    F.trim(F.upper(F.col("Truck_ID"))).alias("truck_id"),
    F.trim(F.upper(F.col("License_Plate"))).alias("license_plate"),
    standardize("Engine_Type", {
        "Diesel":   "Diesel",
        "Gasoline": "Gasoline",
        "Electric": "Electric",
    }).alias("engine_type"),
    F.col("Current_Mileage").cast(IntegerType()).alias("current_mileage_k"),
    F.trim(F.upper(F.col("Truck_ID"))).alias("vehicle_id"),  # FK to Vehicle
)

write_silver(df_trucks, "trucks", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# 3. TRAILERS
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Trailers")
print("="*60)

df = read_bronze("trailers")
bronze_count = df.count()

null_pk = df.filter(F.col("Trailer_ID").isNull()).count()
if null_pk > 0:
    print(f"  ⚠️  DQ WARNING: {null_pk} null Trailer_ID rows — dropping")
    df = df.filter(F.col("Trailer_ID").isNotNull())

# DQ: capacity > 0
invalid_cap = df.filter((F.col("Capacity").isNotNull()) & (F.col("Capacity") <= 0)).count()
if invalid_cap > 0:
    print(f"  ⚠️  DQ WARNING: {invalid_cap} rows with invalid capacity — setting to NULL")
    df = df.withColumn("Capacity",
        F.when(F.col("Capacity") <= 0, None).otherwise(F.col("Capacity")))

df_trailers = df.select(
    F.trim(F.upper(F.col("Trailer_ID"))).alias("trailer_id"),
    F.col("Capacity").cast(DecimalType(10,2)).alias("capacity_lbs"),
    standardize("Trailer_Type", {
        "Flatbed":  "Flatbed",
        "Dryvan":   "Dryvan",
        "Lowboy":   "Lowboy",
        "Stepdeck": "Stepdeck",
    }).alias("trailer_type"),
    F.col("Refrigeration").cast(BooleanType()).alias("has_refrigeration"),
    F.trim(F.upper(F.col("Trailer_ID"))).alias("vehicle_id"),  # FK to Vehicle
)

write_silver(df_trailers, "trailers", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# 4. SERVICE_EVENT
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Service_Event")
print("="*60)

df = read_bronze("service_event")
bronze_count = df.count()

null_pk = df.filter(F.col("Service_Event_ID").isNull()).count()
if null_pk > 0:
    print(f"  ⚠️  DQ WARNING: {null_pk} null Service_Event_ID rows — dropping")
    df = df.filter(F.col("Service_Event_ID").isNotNull())

# DQ: mechanic_cost >= 0
invalid_cost = df.filter((F.col("Mechanic_Cost").isNotNull()) & (F.col("Mechanic_Cost") < 0)).count()
if invalid_cost > 0:
    print(f"  ⚠️  DQ WARNING: {invalid_cost} rows with negative mechanic cost — dropping")
    df = df.filter((F.col("Mechanic_Cost").isNull()) | (F.col("Mechanic_Cost") >= 0))

df_service = df.select(
    F.col("Service_Event_ID").cast(IntegerType()).alias("service_event_id"),
    F.to_timestamp(F.col("Service_Date")).alias("service_date"),
    standardize("Service_Type", {
        "Repair":         "Repair",
        "Maintainence":   "Maintenance",   # fix typo
        "Maintenance":    "Maintenance",
        "Tires":          "Tires",
        "Replace_Part":   "Replace_Part",
    }).alias("service_type"),
    F.trim(F.col("Description")).alias("event_description"),
    standardize("Outcome", {
        "Ready_For_Dispatch":   "Ready_for_Dispatch",
        "Under_Maintainence":   "Under_Maintenance",  # fix typo
        "Under_Maintenance":    "Under_Maintenance",
    }).alias("outcome"),
    F.col("Mechanic_Cost").cast(DecimalType(10,2)).alias("mechanic_cost_usd"),
    F.trim(F.upper(F.col("Vehicle_ID"))).alias("vehicle_id"),  # FK to Vehicle
)

write_silver(df_service, "service_event", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# 5. DRIVER
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Driver")
print("="*60)

df = read_bronze("driver")
bronze_count = df.count()

null_pk = df.filter(F.col("Driver_ID").isNull()).count()
if null_pk > 0:
    print(f"  ⚠️  DQ WARNING: {null_pk} null Driver_ID rows — dropping")
    df = df.filter(F.col("Driver_ID").isNotNull())

# DQ: license expiry sanity check
invalid_expiry = df.filter(
    (F.col("License_Expiry_Date").isNotNull()) &
    (F.to_date(F.col("License_Expiry_Date")) < F.lit("2000-01-01"))
).count()
if invalid_expiry > 0:
    print(f"  ⚠️  DQ WARNING: {invalid_expiry} rows with suspicious license expiry date — flagging")

df_driver = df.select(
    F.col("Driver_ID").cast(IntegerType()).alias("driver_id"),
    F.trim(F.col("First_Name")).alias("first_name"),
    F.trim(F.col("Last_Name")).alias("last_name"),
    F.trim(F.upper(F.col("License_Number"))).alias("license_number"),
    F.to_date(F.col("License_Expiry_Date")).alias("license_expiry_date"),
    standardize("HOS", {
        "Compliant":     "Compliant",
        "Non-Compliant": "Non-Compliant",
    }).alias("hos_status"),
    F.trim(F.col("Phone")).alias("phone"),
    F.lower(F.trim(F.col("Email"))).alias("email"),
    F.trim(F.col("Address")).alias("address"),
    F.initcap(F.trim(F.col("City"))).alias("city"),
    F.trim(F.col("State")).alias("state"),
    F.col("Zip").cast(IntegerType()).alias("zip_code"),
    standardize("Availability", {
        "Available":   "Available",
        "Unavailable": "Unavailable",
    }).alias("availability"),
)

write_silver(df_driver, "driver", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# 6. CLIENT
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Client")
print("="*60)

df = read_bronze("client")
bronze_count = df.count()

null_pk = df.filter(F.col("Client_ID").isNull()).count()
if null_pk > 0:
    print(f"  ⚠️  DQ WARNING: {null_pk} null Client_ID rows — dropping")
    df = df.filter(F.col("Client_ID").isNotNull())

df_client = df.select(
    F.col("Client_ID").cast(IntegerType()).alias("client_id"),
    F.trim(F.col("Company_Name")).alias("company_name"),
    F.trim(F.col("Point_Of_Contact")).alias("point_of_contact"),
    F.trim(F.col("Phone")).alias("phone"),
    F.lower(F.trim(F.col("Email"))).alias("email"),
    F.trim(F.col("Billing_Address")).alias("billing_address"),
)

write_silver(df_client, "client", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# 7. THIRD_PARTY_CARRIER
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Third_Party_Carrier")
print("="*60)

df = read_bronze("third_party_carrier")
bronze_count = df.count()

null_pk = df.filter(F.col("Carrier_ID").isNull()).count()
if null_pk > 0:
    print(f"  ⚠️  DQ WARNING: {null_pk} null Carrier_ID rows — dropping")
    df = df.filter(F.col("Carrier_ID").isNotNull())

df_carrier = df.select(
    F.col("Carrier_ID").cast(IntegerType()).alias("carrier_id"),
    F.trim(F.col("Carrier_Name")).alias("carrier_name"),
    F.trim(F.col("Point_Of_Contact")).alias("point_of_contact"),
    F.trim(F.col("Phone")).alias("phone"),
    F.lower(F.trim(F.col("Email"))).alias("email"),
    standardize("Compliance_Status", {
        "Compliant":     "Compliant",
        "Non-Compliant": "Non-Compliant",
    }).alias("compliance_status"),
)

write_silver(df_carrier, "third_party_carrier", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# 8. TENDER
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Tender")
print("="*60)

df = read_bronze("tender")
bronze_count = df.count()

null_pk = df.filter(F.col("Tender_ID").isNull()).count()
if null_pk > 0:
    print(f"  ⚠️  DQ WARNING: {null_pk} null Tender_ID rows — dropping")
    df = df.filter(F.col("Tender_ID").isNotNull())

# DQ: weight and distance > 0
invalid_weight = df.filter((F.col("Weight").isNotNull()) & (F.col("Weight") <= 0)).count()
invalid_dist   = df.filter((F.col("Distance").isNotNull()) & (F.col("Distance") <= 0)).count()
if invalid_weight > 0:
    print(f"  ⚠️  DQ WARNING: {invalid_weight} rows with invalid weight — dropping")
    df = df.filter((F.col("Weight").isNull()) | (F.col("Weight") > 0))
if invalid_dist > 0:
    print(f"  ⚠️  DQ WARNING: {invalid_dist} rows with invalid distance — dropping")
    df = df.filter((F.col("Distance").isNull()) | (F.col("Distance") > 0))

df_tender = df.select(
    F.col("Tender_ID").cast(IntegerType()).alias("tender_id"),
    F.trim(F.col("Product_Description")).alias("product_description"),
    F.trim(F.col("Pickup_Location")).alias("pickup_location"),
    F.trim(F.col("Dropoff_Location")).alias("dropoff_location"),
    F.to_timestamp(F.col("Expected_Delivery_Date")).alias("expected_delivery_date"),
    standardize("Temp_Requirements", {
        "Ambient":         "Ambient",
        "Frozen":          "Frozen",
        "Deep Frozen":     "Deep Frozen",
        "Deep_Frozen":     "Deep Frozen",
        "Controlled Cool": "Controlled Cool",
        "Controlled_Cool": "Controlled Cool",
    }).alias("temp_requirements"),
    F.col("Weight").cast(DecimalType(10,2)).alias("weight_lbs"),
    F.col("Distance").cast(DecimalType(10,2)).alias("distance_miles"),
    F.col("Client_ID").cast(IntegerType()).alias("client_id"),  # FK to Client
)

write_silver(df_tender, "tender", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# 9. SHIPMENT
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Shipment")
print("="*60)

df = read_bronze("shipment")
bronze_count = df.count()

# DQ: uniqueness check on PK
dup_pk = df.groupBy("Shipment_ID").count().filter(F.col("count") > 1).count()
if dup_pk > 0:
    print(f"  ❌ DQ FAIL: {dup_pk} duplicate Shipment_ID values found")
else:
    print(f"  ✅ DQ PASS: No duplicate Shipment_IDs")

# DQ: distance > 0
invalid_dist = df.filter((F.col("Distance").isNotNull()) & (F.col("Distance") <= 0)).count()
if invalid_dist > 0:
    print(f"  ⚠️  DQ WARNING: {invalid_dist} rows with invalid distance — dropping")
    df = df.filter((F.col("Distance").isNull()) | (F.col("Distance") > 0))

# DQ: consistency check — temp_requirements vs Tender
df_tender_check = spark.read.parquet(os.path.join(SILVER_PATH, "tender"))
df_consistency = df.join(
    df_tender_check.select("tender_id", F.col("temp_requirements").alias("tender_temp")),
    df["Tender_ID"] == df_tender_check["tender_id"], "left"
).filter(
    (F.col("Temp_Requirements").isNotNull()) &
    (F.col("tender_temp").isNotNull()) &
    (F.col("Temp_Requirements") != F.col("tender_temp"))
)
inconsistent = df_consistency.count()
if inconsistent > 0:
    print(f"  ⚠️  DQ WARNING: {inconsistent} shipments where Temp_Requirements differs from Tender — flagged")

df_shipment = df.select(
    F.col("Shipment_ID").cast(IntegerType()).alias("shipment_id"),
    F.trim(F.col("Pickup_Point")).alias("pickup_point"),
    F.trim(F.col("Delivery_Point")).alias("delivery_point"),
    standardize("Delivery_Status", {
        "Delivered":  "Delivered",
        "Pending":    "Pending",
        "In_Transit": "In_Transit",
        "In Transit": "In_Transit",
    }).alias("delivery_status"),
    F.to_timestamp(F.col("Delivery_Date")).alias("delivery_date"),
    standardize("Temp_Requirements", {
        "Ambient":         "Ambient",
        "Frozen":          "Frozen",
        "Deep Frozen":     "Deep Frozen",
        "Deep_Frozen":     "Deep Frozen",
        "Controlled Cool": "Controlled Cool",
        "Controlled_Cool": "Controlled Cool",
    }).alias("temp_requirements"),
    F.col("Assigned_Driver_ID").cast(IntegerType()).alias("assigned_driver_id"),
    F.col("Distance").cast(DecimalType(10,2)).alias("distance_miles"),
    F.col("Tender_ID").cast(IntegerType()).alias("tender_id"),  # FK to Tender
)

write_silver(df_shipment, "shipment", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# 10. INTERNAL_SHIPMENT
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Internal_Shipment")
print("="*60)

df = read_bronze("internal_shipment")
bronze_count = df.count()

# DQ: fuel_cost >= 0
invalid_fuel = df.filter((F.col("Fuel_Costs").isNotNull()) & (F.col("Fuel_Costs") < 0)).count()
if invalid_fuel > 0:
    print(f"  ⚠️  DQ WARNING: {invalid_fuel} rows with negative fuel cost — dropping")
    df = df.filter((F.col("Fuel_Costs").isNull()) | (F.col("Fuel_Costs") >= 0))

df_internal = df.select(
    F.col("Internal_Shipment_ID").cast(IntegerType()).alias("internal_shipment_id"),  # PK + FK
    F.col("Fuel_Costs").cast(DecimalType(10,2)).alias("fuel_cost_usd"),
)

write_silver(df_internal, "internal_shipment", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# 11. BROKERED_SHIPMENT
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Brokered_Shipment")
print("="*60)

df = read_bronze("brokered_shipment")
bronze_count = df.count()

# DQ: rate > 0, margin >= 0
invalid_rate   = df.filter((F.col("Rate").isNotNull()) & (F.col("Rate") <= 0)).count()
invalid_margin = df.filter((F.col("Profit_Margin").isNotNull()) & (F.col("Profit_Margin") < 0)).count()
if invalid_rate > 0:
    print(f"  ⚠️  DQ WARNING: {invalid_rate} rows with invalid rate — dropping")
    df = df.filter((F.col("Rate").isNull()) | (F.col("Rate") > 0))
if invalid_margin > 0:
    print(f"  ⚠️  DQ WARNING: {invalid_margin} rows with negative profit margin — flagging")

df_brokered = df.select(
    F.col("Brokered_Shipment_ID").cast(IntegerType()).alias("brokered_shipment_id"),  # PK + FK
    standardize("B_Status", {
        "Delivered": "Delivered",
        "Pending":   "Pending",
    }).alias("brokered_status"),
    F.col("Profit_Margin").cast(DecimalType(10,2)).alias("profit_margin_usd"),
    F.col("Rate").cast(DecimalType(10,2)).alias("rate_usd"),
    F.col("Carrier_ID").cast(IntegerType()).alias("carrier_id"),  # FK to Third_Party_Carrier
)

write_silver(df_brokered, "brokered_shipment", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# 12. DISPATCH
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("TRANSFORMING: Dispatch")
print("="*60)

df = read_bronze("dispatch")
bronze_count = df.count()

# DQ: composite PK uniqueness
dup_pk = df.groupBy("Shipment_ID", "Driver_ID", "Truck_ID").count().filter(F.col("count") > 1).count()
if dup_pk > 0:
    print(f"  ❌ DQ FAIL: {dup_pk} duplicate composite PK rows found")
else:
    print(f"  ✅ DQ PASS: No duplicate composite PKs")

# DQ: required FKs not null
for col in ["Shipment_ID", "Driver_ID", "Truck_ID"]:
    null_count = df.filter(F.col(col).isNull()).count()
    if null_count > 0:
        print(f"  ❌ DQ FAIL: {null_count} null values in required FK {col} — dropping")
        df = df.filter(F.col(col).isNotNull())

df_dispatch = df.select(
    F.col("Shipment_ID").cast(IntegerType()).alias("shipment_id"),   # PK + FK → Internal_Shipment
    F.col("Driver_ID").cast(IntegerType()).alias("driver_id"),       # FK → Driver
    F.trim(F.upper(F.col("Truck_ID"))).alias("truck_id"),           # FK → Trucks
    F.trim(F.upper(F.col("Trailer_ID"))).alias("trailer_id"),       # FK → Trailers (nullable)
)

write_silver(df_dispatch, "dispatch", bronze_count)

# ─────────────────────────────────────────────────────────────────────────────
# RECONCILIATION REPORT
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("RECONCILIATION REPORT")
print("="*60)

passed = sum(1 for r in reconciliation if r["status"] == "PASS")
failed = sum(1 for r in reconciliation if r["status"] == "FAIL")

for r in reconciliation:
    flag = "✅" if r["status"] == "PASS" else "❌"
    print(f"  {flag} {r['table']:<30} Bronze: {r['bronze_count']:<6} Silver: {r['silver_count']:<6} {r['status']}")

print(f"\n  Total: {len(reconciliation)} tables | Passed: {passed} | Failed: {failed}")

with open(LOG_PATH, "w") as f:
    json.dump(reconciliation, f, indent=2)
print(f"\n  📄 Reconciliation log saved to: {LOG_PATH}")

print("\n✅ Silver transformation complete.\n")
spark.stop()
