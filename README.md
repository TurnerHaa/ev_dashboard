# UK Electric Vehicles Data Pipeline
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffffff)
![Supabase](https://img.shields.io/badge/supabase-3FCF8E?style=for-the-badge&logo=supabase&logoColor=ffffff)
![Pandas](https://img.shields.io/badge/pandas-150458?style=for-the-badge&logo=pandas&logoColor=ffffff)
![Postgres](https://img.shields.io/badge/postgresql-4169E1?style=for-the-badge&logo=postgresql&logoColor=ffffff)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=power-bi&logoColor=black)

![2026-03-18-17-32-57](https://github.com/user-attachments/assets/30dd8d64-5168-4f4d-837e-f62fd4207880)

### The Problem: 
1 in 5 vehicles sold in the UK are now electric, but the infrastructure rollout risks stalling. Vital data for policymakers is currently locked in dense, quarterly GOV.UK reports, making it difficult to track regional disparities or real-time progress.

### The Solution: 
This ETL pipeline automatically ingests quarterly and monthly DfT (Department for Transport) statistics, transforms them into a unified schema, and feeds a Power BI Dashboard. This allows anyone—from local councillors to EV advocates—to visualize charger density and vehicle adoption across all UK regions.

## Process
<img width="1640" height="746" alt="ETL" src="https://github.com/user-attachments/assets/31cd4315-ff1e-4153-b988-3ea84a9ee837" />

## 🛠 Tech Stack
\* **Data collection + cleaning:** (Python + Pandas + Beautiful Soup) — Pulling data from GOV.UK, cleaning notes and mispellings, reformatting dates. 

\* **Load:** (SQLAlchemy + dbt) — Connecting to Supabase and creating atomized data tables and region_closure for hierarchical data.

\* **Database:** (Supabase/PostgreSQL) — Simple to manage and generous free tier for a small project.

\* **Visualization:** (PowerBI) — Used to visualize data and explore core insights.

\* **Scheduling:** (GitHub Actions) — Automatically check for new data and re-running the pipeline when new values are found.


## ⚡ Getting started
Steps to get the repo up and running:

```Bash
# Clone the repository
git clone https://github.com/TurnerHaa/ev_dashboard.git

cd your-name

# Install dependencies
pip install -r requirements.txt

```




## 🏔️ Challenges
#### Ragged hierarchy ####
PowerBI lacks an easy way to deal with ragged hierarchies despite them being critical for users to easily switch between different tiers of UK geographies and build an analysis that matches their decision making range.

To make this work, we must ensure the value of a wider area (like the entire UK) is always just the sum of the areas at the lowest levels. If our data contains the actual UK values, we overinflate our total. 

The solution was to create a filtered hierarchy table. Here, each row represents a distint UK region and columns *level1* to *level6* indicate the 'family tree' leading to that respective region.

<img width="2118" height="766" alt="image" src="https://github.com/user-attachments/assets/a2eff881-94e6-4573-b1b9-1920f542097f" />

For this hierarchy to work in the data model, all fact tables needed to be atomized so they only contain the smallest regions in the hierarchy – i.e. the smallest regional slices in the UK. This was done using recursive CTEs.

We connect these to our filtered hierarchy using a closure table, that lists every existing link between an ancestor and descendant in the entire tree.

```SQL
SELECT
    c.region_ons,
    c.quarter,
    c.all_chargers,
    c.fast_chargers
FROM raw_data c
WHERE NOT EXISTS (
    SELECT 1
    FROM closure rc
    JOIN raw_data c_child
        ON c_child.region_ons = rc.descendant_ons
        AND c_child.quarter = c.quarter
    WHERE rc.ancestor_ons = c.region_ons
    AND rc.descendant_ons <> c.region_ons
)

```

    


## 📊 Key insights


## 💡Future expansion
\* Add a second report page with visualizations tailored to specific UK regions for advanced intraregional comparisons

\* Add mobile alerts for data updates using Twilio

\* Allow toggle between total chargers and chargers per 100k on primary bar chart

\* Partition chargers and vehicles tables for faster recursive CTE passovers

\* Build views for more focused data ingestion from Supabase

\* Turn repeated Python code into functions to improve codebase structure

\* Create separate page for chargers data which reports more frequently than EV licensing data
