# GamePulse Data Warehouse & Analytics Stack

**GamePulse** is a full-scale sports analytics data warehouse project built to model a modern ELT/ETL pipeline using the **Bronze → Silver → Gold** architecture pattern.  
The project simulates a professional data engineering environment — transforming raw sports and business datasets into clean, analytical-ready tables for reporting and insights.

---

## Project Overview

The GamePulse warehouse is designed to demonstrate:
- End-to-end **data ingestion, transformation, and modeling**
- Implementation of the **medallion architecture** (Bronze → Silver → Gold)
- Integration of **SQL Direct ETL scripts**, **SQL Server stored procedures**, and **Business Logic Analytics**
- Reusable **dimensional modeling patterns** for both sports and business data

This environment supports ongoing simulation and analytics across multiple sports and organizations — with the current build centered on **GamePulse Core Data Models**.

---

## 🧱 Architecture Overview

───────────────────────────────────────────────────────┐
│ BRONZE │
│ Raw data layer — direct ingests, no transformations
│ Naming convention bronze.source_table
│ Tables: sm_customer, sm_ticket_orders, sm_campaigns │
└───────────────────────────────────────────────────────┘
│
▼
┌───────────────────────────────────────────────────────┐
│ SILVER │
│ Cleansed & conformed data — standard naming, types, │
│ and business keys applied │
| Naming convention: silver.source_table
│ Tables: same as bronze layer │
└───────────────────────────────────────────────────────┘
│
▼
┌───────────────────────────────────────────────────────┐
│ GOLD │
│ Analytics layer — aggregated fact and dim tables│
│ Start Schema│
└───────────────────────────────────────────────────────┘
