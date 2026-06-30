# New York Rideshare Analysis Using PySpark

## Overview

This project analyses NYC Uber/Lyft rideshare trips from January to May 2023 using Apache Spark. It covers data joining and cleaning, trip and earnings aggregation, route profitability, and graph-based analysis of trip corridors between zones.

---

## Project Structure

```
nyc-rideshare-spark/
├── data/
│   ├── rideshare_data.csv
│   └── taxi_zone_lookup.csv
├── scripts/
│   ├── dataanalysis.py
│   └── graphanalysis.py
├── notebooks/
│   └── visualisations.ipynb
├── output_files/
└── README.md
```

---

## Dataset

- **rideshare_data.csv** — trip-level data structured on NYC TLC's public rideshare schema, including pickup/dropoff zone IDs, trip distance and duration, wait time, fare, tips, driver pay, and platform profit, covering Jan–May 2023.
- **taxi_zone_lookup.csv** — maps zone IDs to borough and zone names, used to enrich trip records with location detail.
- **joined_df.csv** (generated) — the merged dataset combining trip data with borough/zone information, produced by `dataanalysis.py`.

---

## File Descriptions

**`scripts/dataanalysis.py`**
Loads and joins the trip and zone lookup datasets, then runs:
- Monthly trip counts, driver earnings, and platform profit by business
- Most profitable pickup→dropoff borough routes
- Top 10 pickup and dropoff zones
- Average wait time by platform for January

**`scripts/graphanalysis.py`**
Builds on the joined dataset to perform:
- Route corridor frequency analysis (most common pickup→dropoff zone pairs)
- A graph model representing zones as vertices and trips as weighted edges

**`notebooks/visualisations.ipynb`**
Loads the output CSVs and produces charts: monthly trip volume, driver earnings comparison, top pickup zones, and average wait time by platform.

**`output_files/`**

| File | Description |
|---|---|
| `joined_df.csv` | Trip data merged with pickup/dropoff borough and zone names |
| `monthly_summary.csv` | Trip counts, driver earnings, and platform profit by business and month |
| `profitable_routes.csv` | Trip count and total profit by pickup→dropoff borough pair |
| `top_pickups.csv` | Top 10 pickup zones by trip count |
| `top_dropoffs.csv` | Top 10 dropoff zones by trip count |
| `avg_waiting_time.csv` | Average wait time by platform for January |
| `route_corridors.csv` | Trip count for each pickup→dropoff zone pair |
| `graph_vertices.csv` | Unique zones used as graph vertices |
| `graph_edges.csv` | Weighted edges between zones, based on trip frequency |
| `*.png` | Charts generated from the above, produced by `visualisations.ipynb` |

---