# 🚗 UK Electric Vehicles Data Pipeline
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffffff)
![Supabase](https://img.shields.io/badge/supabase-3FCF8E?style=for-the-badge&logo=supabase&logoColor=ffffff)
![Pandas](https://img.shields.io/badge/pandas-150458?style=for-the-badge&logo=pandas&logoColor=ffffff)
![Postgres](https://img.shields.io/badge/postgresql-4169E1?style=for-the-badge&logo=postgresql&logoColor=ffffff)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=power-bi&logoColor=black)

An end-to-end ETL pipeline that extracts, cleans, and visualizes the latest electric vehicle (EV) registrations and charging infrastructure data from the UK Department for Transport (DfT).

VIEW THE LIVE DASHBOARD

![2026-03-27 20-45-24](https://github.com/user-attachments/assets/020ef005-8393-42f7-b563-bbc44413d4ae)

### 📖 The Problem: 
While 1 in 5 vehicles sold in the UK are now electric, infrastructure rollout risks stalling. Vital data for policymakers is often locked in **dense, quarterly Excel/CSV reports**, making it difficult to track regional disparities or real-time progress.

### 💡 The Solution: 
This pipeline automates the ingestion of quarterly DfT statistics, feeding into a **Power BI Dashboard**. This enables local councillors and EV advocates to visualize charger density and vehicle adoption across all UK regions instantly.

## Process
<img width="1177" height="508" alt="Screenshot 2026-03-23 200122" src="https://github.com/user-attachments/assets/e2583c6f-dad8-45f6-a3d1-116ae9a03ce9" />


## 🛠 Tech Stack
| Layer | Tools | Purpose |
| :--- | :--- | :--- |
| **Extraction** | `Python`, `Beautiful Soup` | Scraping GOV.UK for the latest release links. |
| **Transformation** | `Pandas`,`dbt` | Data cleaning, date formatting, and hierarchical modeling. |
| **Loading** | `SQLAlchemy` | Direct ingestion into the cloud warehouse. |
| **Database** | `Supabase (PostgreSQL)` | Scalable storage with a managed relational schema. |
| **Orchestration** | `GitHub Actions` | CRON-scheduled runs (Mon/Fri) to check for data updates. |
| **Visualization** | `PowerBI` | Interactive spatial and trend analysis. |

## 🔧 Data Engineering Challenges
**1. Solving the ragged hierarchy**
The UK's regional geography can be messy. To prevent "double counting" in Power BI:

**The problem:** If the dashboard sums "London" and "Westminster" a subset of London, the totals overinflate.

**The fix:** I introduced a closure table and recursive CTE. All fact tables are atomized to the smallest possible regional slice.

**The result:** Users can drill down from "National" to "Local" without double-counting data.

```SQL
SELECT
    c.region_ons, c.quarter, c.all_chargers, c.fast_chargers
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
<img width="2297" height="1036" alt="schema" src="https://github.com/user-attachments/assets/a0c2f97a-8163-4aa3-9e1a-739f467dcadc" />

**2. Synchronizing data release dates**
**The problem:** Charger data and EV registrations release on different schedule to each other. 
**The fix:** Pipeline logic uses vehichle registrations as the 'global date', preventing nulls from lagging data releases.

## 📊 Key Insights (As of Q3 2025)
**Growth:** Total public chargers increased 4x since 2019 (15k → 82k).

**The "Gap":** Windsor & Maidenhead show the largest imbalance with 1,020 vehicles per charger.

**Efficiency:** Coventry and Hackney lead the country with a 3:1 vehicle-to-charger ratio.

**Infrastructure Mix:** Rapid/Ultra-rapid chargers doubled in volume (8k → 16k) but their total market share only grew by 2%, indicating slower-speed chargers still dominate the rollout.

## 🚀 Getting started

**Prerequisites**

+ Python 3.9+

+ Supabase account

+ PowerBI

+ dbt Core (for running local transformations)

**Setup**
**1. Clone and install**
```Bash
# Clone the repository
git clone https://github.com/TurnerHaa/ev_dashboard.git

cd your-project

# Install dependencies
pip install -r requirements.txt
```

**2. Environment variables**
Create a `.env` file based on `examples.env` or setup your GitHub actions with the following secrets:

```YAML
env:
  DB_HOST: ${{ secrets.DB_HOST }}
  DB_USER: ${{ secrets.DB_USER }}
  DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
  DB_NAME: ${{ secrets.DB_NAME }}
  DB_PORT: "6543"

```
**3. Database schema**
The dbt models are located in `/models`. Run `dbt run` to create hierarchy and closure tables.


<details>
<summary><b>View GitHub Actions time settings</b></summary>

```yaml
on:
  schedule:
    - cron: '00 00 * * 1,5' # set to run the pipeline at midnight every Monday and Friday.
```
</details>

## Future roadmap

[ ] Mobile Alerts: Integrate Twilio for SMS notifications when new DfT data is detected.

[ ] Optimization: Partition chargers and vehicles tables for faster recursive queries.

[ ] Advanced Viz: Add a "Comparison Mode" to benchmark two specific Local Authorities side-by-side.
