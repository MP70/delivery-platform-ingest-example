# Order Data Ingestion Platform

Minimal order data ingestion and analysis example. Please don't use with untrusted csvs, or in a production/serious capacity. Three **unnamed** popular delivery plaforms.

## Quick Start

```bash
# Setup database
psql -d orders_db -f SCHEMA.sql

# Seed integrations
npx tsx seed-integrations.ts
```
## CLI Usage

```bash
# Process w Files
npx tsx cli.ts process "data.csv"

# Process w specific integration
npx tsx cli.ts process "data.csv" deliveryplatform3_total_order

# Run some example analysis
npx tsx cli.ts analyse
``` 