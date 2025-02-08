# Models Folder - SQL Scripts

## Overview
This folder contains SQL scripts for processing and handling data across different layers of the **Medallion Architecture** within a **dbt (Data Build Tool) environment**:
- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Data cleansing and transformation
- **Gold Layer**: Aggregated and enriched data for analytics
- **Snapshots**: Historical tracking of records

## File Placement

### Bronze Layer (`Models/Bronze/`)
Ensure that the following files are placed in the `Models/Bronze/` directory:

| File Name        | Description |
|-----------------|-------------|
| `b_censusg1.sql`   | SQL script for processing Census Group 1 data |
| `b_censusg2.sql`   | SQL script for processing Census Group 2 data |
| `b_lgacode.sql`    | SQL script for processing Local Government Area (LGA) codes |
| `b_lgasuburb.sql`  | SQL script for processing LGA and suburb mapping |

### Silver Layer (`Models/silver/`)
Ensure that the following files are placed in the `Models/silver/` directory:

| File Name        | Description |
|-----------------|-------------|
| `s_censusg1.sql`   | SQL script for refined Census Group 1 data |
| `s_cemsusg2.sql`   | SQL script for refined Census Group 2 data |
| `s_lgacode.sql`    | SQL script for refined Local Government Area (LGA) codes |
| `s_lgasuburb.sql`  | SQL script for refined LGA and suburb mapping |
| `s_listing.sql`    | SQL script for refined property listing data |

### Gold Layer - Mart (`Models/gold/mart/`)
Ensure that the following files are placed in the `Models/gold/mart/` directory:

| File Name                           | Description |
|-------------------------------------|-------------|
| `g_dm_host_neighbourhood.sql`      | SQL script for dimensional modeling of host-neighbourhood relationships |
| `g_dm_listing_neighbourhood.sql`   | SQL script for dimensional modeling of listing-neighbourhood relationships |
| `g_dm_property_type.sql`           | SQL script for dimensional modeling of property types |

### Gold Layer - Star Schema (`Models/gold/star/`)
Ensure that the following files are placed in the `Models/gold/star/` directory:

| File Name                | Description |
|-------------------------|-------------|
| `g_dim_host.sql`        | SQL script for dimensional modeling of hosts |
| `g_dim_property.sql`    | SQL script for dimensional modeling of properties |
| `g_dim_review.sql`      | SQL script for dimensional modeling of reviews |
| `g_dim_scd.sql`         | SQL script for slowly changing dimensions (SCD) |
| `g_fact_listings.sql`   | SQL script for fact table of listings |

### Snapshots (`Snapshots/`)
Ensure that the following files are placed in the `Snapshots/` directory:

| File Name                | Description |
|-------------------------|-------------|
| `listing_snapshot.sql`  | SQL script for capturing historical changes in listing data |

## Usage
These scripts should be executed in their respective layers in the following order to ensure proper data dependencies:
1. **Bronze Layer:**
   - `b_censusg1.sql`
   - `b_censusg2.sql`
   - `b_lgacode.sql`
   - `b_lgasuburb.sql`
2. **Silver Layer:**
   - `s_censusg1.sql`
   - `s_cemsusg2.sql`
   - `s_lgacode.sql`
   - `s_lgasuburb.sql`
   - `s_listing.sql`
3. **Gold Layer - Mart:**
   - `g_dm_host_neighbourhood.sql`
   - `g_dm_listing_neighbourhood.sql`
   - `g_dm_property_type.sql`
4. **Gold Layer - Star Schema:**
   - `g_dim_host.sql`
   - `g_dim_property.sql`
   - `g_dim_review.sql`
   - `g_dim_scd.sql`
   - `g_fact_listings.sql`
5. **Snapshots:**
   - `listing_snapshot.sql`

### Execution Instructions
- Run the SQL scripts in a **dbt environment**.
- Ensure that raw data sources are available before execution.
- Validate data integrity after execution.

## Notes
- Any updates or new scripts should follow the naming conventions:
  - `b_<dataset>.sql` for Bronze Layer.
  - `s_<dataset>.sql` for Silver Layer.
  - `g_dm_<dataset>.sql` for Gold Mart Layer.
  - `g_dim_<dataset>.sql` and `g_fact_<dataset>.sql` for Gold Star Schema.
  - `listing_snapshot.sql` for historical tracking in Snapshots.
- Ensure proper indexing and partitioning for optimized queries.

---

