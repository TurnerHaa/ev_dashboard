# UK Electric Vehicles Data Pipeline
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffffff)
![Supabase](https://img.shields.io/badge/supabase-3FCF8E?style=for-the-badge&logo=supabase&logoColor=ffffff)
![Pandas](https://img.shields.io/badge/pandas-150458?style=for-the-badge&logo=pandas&logoColor=ffffff)
![Postgres](https://img.shields.io/badge/postgresql-4169E1?style=for-the-badge&logo=postgresql&logoColor=ffffff)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=power-bi&logoColor=black)

An end-to-end ETL pipeline that extracts, cleans, and visualizes the latest electric vehicle (EV) registrations and charging infrastructure data from the UK Department for Transport (DfT).

VIEW THE LIVE DASHBOARD

![2026-03-18-17-32-57](https://github.com/user-attachments/assets/30dd8d64-5168-4f4d-837e-f62fd4207880)

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
**Solving the ragged hierarchy**
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



## ⚡ Getting started

**Prerequisites**

**Setup**

To begin using the project yourself, clone the repository and ensure you have all the necessary requirements.

```Bash
# Clone the repository
git clone https://github.com/TurnerHaa/ev_dashboard.git

cd your-project

# Install dependencies
pip install -r requirements.txt

```

You will also need to create a Supabase project (or your preferred cloud database) where your cleaned data will be stored. 

By default, the script pulls database credentials from GitHub Secrets (Settings > Secrets and variables > actions). To run locally, you can substitute secrets with local environment variables. See examples.env for reference.


```YAML
env:
  DB_HOST: ${{ secrets.DB_HOST }}
  DB_USER: ${{ secrets.DB_USER }}
  DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
  DB_NAME: ${{ secrets.DB_NAME }}
  DB_PORT: "6543"

```

If running this pipeline in a GitHub repo, profiles is stored as another repo secret which is accessed by pipeline.yml.

```
- name: Setup dbt Profile
        run: |
          mkdir -p ~/.dbt
          echo "${{ secrets.DBT_PROFILES }}" > ~/.dbt/profiles.yml
```

On a local machine, the equivalent file is found under Users/your_username/.dbt/profiles.yml.

See profiles_example.yml for an example of what database information should be stored in your profile variable.

**Supabase schema**
<img width="2297" height="1036" alt="schema" src="https://github.com/user-attachments/assets/a0c2f97a-8163-4aa3-9e1a-739f467dcadc" />



**Time configuration**

By default, this CRON job runs the script via GitHub actions at midnight on Monday and Friday.

You can change how frequently the pipeline will search for new data inside workflows/pipeline.yml.

Keep in mind, GitHub actions is limited to 2,000 minutes per month across all projects for free accounts.



```YAML
on:
  schedule:
    - cron: '00 00 * * 1,5'
```


## 🏔️ Challenges
#### Ragged hierarchy ####
PowerBI lacks an easy way to deal with ragged hierarchies despite them being critical for users to easily switch between different tiers of UK geographies and build an analysis that matches their decision making range.

To make this work, we must ensure the value of a wider area (like the entire UK) is always just the sum of the areas at the lowest levels. If our data contains the actual UK values, we overinflate our total. 

The solution was to create a filtered hierarchy table. Here, each row represents a distint UK region and columns *level1* to *level6* indicate the 'family tree' leading to that respective region.

<img width="2118" height="766" alt="image" src="https://github.com/user-attachments/assets/a2eff881-94e6-4573-b1b9-1920f542097f" />

For this hierarchy to work in the data model, all fact tables needed to be atomized so they only contain the smallest regions in the hierarchy – i.e. the smallest regional slices in the UK. This was done using recursive CTEs.

We connect these to our filtered hierarchy using a closure table, that lists every existing link between an ancestor and descendant in the entire tree.

This decision came with a trade off. In prioritising the user's ability to hop between geographic scales it became easier to look at data at local, regional, country and national scales, but it became harder to implement comparisons like averages into charts because the comparison would shift drastically with geographic filters. This is something that could be addressed in future iterations.

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

#### Date ranges  ####
Although the UK governments Department for Transport handles the data for both EV charging infrastructure and vehicle registrations and both are reported quarterly, new releases for both datasets do not arrive on the same day. 

In order to keep valuable insights like vehicle to charger ration, the decision was made to set the latest dashboard date to latest date in the vehicles data (tends to release slower) to preserve the dashboards full suite of insights at any given time.

## 📊 Key insights

\* Since Q4 2019, the total number of public chargers available nationwide increased four-fold, from 15,000 to 82,000.

\* In Windsor and Maidenhead, EV adoption has outpaced charger infrastructure. With 158,000 registered EVs and just 155 public chargers, the area has the largest imbalance in the country with 1,020 vehicles per charger.

\* Coventry and Hackney tied for the best vehicle to charger ratio with just three registered electric vehicles per public charger.

\* London leads comfortably in terms of chargers per population, with 275 EV chargers per 100,000 people. The second highest, West Midlands, sat at less than half this number at 127 chargers per 100k.

\* Between Q4 2023 (earliest) and Q3 2025 (latest) the total number of rapid + ultra rapid chargers roughly doubled from 8,000 to 16,000, however their total share of charging infrastructure only rose from 18% to 20%.


## 💡Future expansion
\* Add a second report page with visualizations tailored to specific UK regions for advanced intraregional comparisons

\* Add mobile alerts for data updates using Twilio

\* Allow toggle between total chargers and chargers per 100k on primary bar chart

\* Partition chargers and vehicles tables for faster recursive CTE passovers

\* Build views for more focused data ingestion from Supabase

\* Turn repeated Python code into functions to improve codebase structure

\* Create separate page for chargers data which reports more frequently than EV licensing data
