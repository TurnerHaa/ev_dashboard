# UK Electric Vehicles Data Pipeline
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffffff)
![Supabase](https://img.shields.io/badge/supabase-3FCF8E?style=for-the-badge&logo=supabase&logoColor=ffffff)
![Pandas](https://img.shields.io/badge/pandas-%150458?style=for-the-badge&logo=pandas&logoColor=ffffff)
![Postgres](https://img.shields.io/badge/postgresql-%4169E1?style=for-the-badge&logo=postgres&logoColor=ffffff)


![2026-03-18-17-32-57](https://github.com/user-attachments/assets/30dd8d64-5168-4f4d-837e-f62fd4207880)

### The Problem: 
1 in 5 vehicles sold in the UK are now electric, but the infrastructure rollout risks stalling. Vital data for policymakers is currently locked in dense, quarterly GOV.UK reports, making it difficult to track regional disparities or real-time progress.

### The Solution: 
This ETL pipeline automatically ingests quarterly and monthly DfT (Department for Transport) statistics, transforms them into a unified schema, and feeds a Power BI Dashboard. This allows anyone—from local councillors to EV advocates—to visualize charger density and vehicle adoption across all UK regions.

## Process
<img width="1640" height="746" alt="ETL" src="https://github.com/user-attachments/assets/31cd4315-ff1e-4153-b988-3ea84a9ee837" />

## 🛠 Tech Stack
Data scraping + cleaning: Python + Beautiful Soup — Chosen for near-instant HMR (Hot Module Replacement) during development.

Load: SQLAlchemy + dbt — PLACEHOLDER

Database: Supabase (PostgreSQL) — Selected over Redux to keep the boilerplate minimal for this specific scale of state complexity.

Visualization: PowerBI — Used to leverage Row Level Security (RLS) and real-time subscriptions without building a custom backend.
