# EECA Dashboard Source Code

This repo contains one SQL script (written on Postgres) that is responsible for the logic to combine the necessary data sources and form the base table for the EECA Community EV Charger Planner dashboard.

The script is: 

create estimated future ev numbers by sa2.sql


# Process

The overall process involves
-	Ingest updated data
-	Run script in postgres to transform and update calculations
-	Bring tables/data into ArcGIS pro
-	Replace tables/layers in AGOL

# Data sources

Regular data updates come from three main sources:

## EECA 
- The existing and planned charge points
- Public points 
- Pipeline points

These get supplied as CSV files that need no editing other than rename the files.

## NZTA
The existing number of vehicles

## Stats NZ
- SA2 boundaries
- SA2 population projections
- SA2 demographics

# Database
The AGOL layers depend on a flattened table the combines all of the above data sources into a single data source. The script "create estimated future ev numbers by sa2.sql" transforms the data and runs a series of calculations to estimate future EV uptake by SA2 based on current distribution and population projections. Assuming an appropriate staging DB is created, this script is the primary source of logic to merge the data.

The remainder of the application is entirely within AGOL.

# AGOL Components (not in this Repo)

- AGOL layers/features used by the dashboard:
- EECA web map_3 (webmap)
- EECA community EV Charger planner (Dashboard)
- sa2_summary_geo (this is a view generated from  below)
- Pipeline_points (hosted feature layer)
- Public_points (hosted feature layer)
- Excluded_areas (hosted feature layer)
- Lynker_pg_main_eeca_ev_summary (table hosted, data is copied from the database mentioned above)

