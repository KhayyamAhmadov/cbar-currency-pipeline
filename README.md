# 💱 Currency Data Pipeline (CBAR.az Scraper & Power BI Dashboard)

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Airflow](https://img.shields.io/badge/Airflow-Automation-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue)
![Docker](https://img.shields.io/badge/Docker-Containerization-lightblue)
![PowerBI](https://img.shields.io/badge/PowerBI-Visualization-yellow)

---

## 📘 Project Overview
This project automates the **collection, storage, and visualization** of daily currency exchange rates from the **Central Bank of Azerbaijan (CBAR.az)**.  
It demonstrates a complete **data engineering pipeline** — from **web scraping** to **database management**, **workflow automation**, and **business intelligence visualization**.

The system collects currency data starting from **1993** and keeps updating daily through an **Apache Airflow** pipeline running inside **Docker**.

---

## ⚙️ Tech Stack

| Component | Technology |
|------------|-------------|
| **Data Source** | [CBAR.az](https://www.cbar.az) |
| **Scraping** | `Python`, `Requests`, `BeautifulSoup` |
| **Workflow Orchestration** | `Apache Airflow` |
| **Database** | `PostgreSQL` |
| **Visualization** | `Power BI` |
| **Containerization** | `Docker`, `docker-compose` |

---

## 🗂️ Database Schema

**Table:** `exchange_rates`

| Column | Type | Description |
|---------|------|-------------|
| `id` | `serial` | Primary key |
| `date` | `date` | Exchange rate date |
| `name` | `varchar(50)` | Currency name (AZN-based) |
| `code` | `varchar(5)` | Currency code (e.g., USD, EUR, GBP) |
| `rate` | `numeric(12,4)` | Exchange rate value |

**Example SQL:**
```sql
CREATE TABLE exchange_rates (
  id SERIAL PRIMARY KEY,
  date DATE,
  name VARCHAR(50),
  code VARCHAR(5),
  rate NUMERIC(12,5)
);
```

---

## 🚀 Airflow in This Project

In this project, **Apache Airflow** is used to fully automate the **daily collection of currency data** from the Central Bank of Azerbaijan (CBAR.az).  
All scraping and database update tasks are managed and executed automatically by Airflow.

### 🔁 How It Works
- **Daily Scheduling:**  
  Airflow triggers the scraping task once a day, ensuring that the system always has the latest currency rates.
  
- **Incremental Updates:**  
  The DAG checks the last collected date and only scrapes data for missing days, preventing duplicate entries in the PostgreSQL database.
  
- **Database Integration:**  
  After scraping, data is inserted directly into the `exchange_rates` table in PostgreSQL.
  
- **Automatic Recovery:**  
  If the website is temporarily unavailable, Airflow retries the task automatically on the next scheduled run.
  
- **Monitoring and Logs:**  
  All scraping runs, task statuses, and logs can be tracked through the Airflow Web UI (`http://localhost:8080`).

### 🧠 Why Airflow
Using Airflow allows the project to:
- Run without manual intervention  
- Keep data consistent and up to date  
- Scale easily by adding new DAGs (for example, for additional data sources or transformations)

Overall, Airflow ensures that **the entire ETL process runs smoothly and reliably every day**.

---

## 🐳 Docker in This Project

The whole project runs inside **Docker containers**, making it easy to deploy, run, and manage — without needing to install anything manually.

### 🧩 Components Inside Docker
- **PostgreSQL Container:**  
  Hosts the `currency` database where all scraped exchange rates are stored.  
  The data is persistent thanks to Docker volumes, so it is not lost after restarting containers.
  
- **Airflow Container:**  
  Runs all automation workflows — the scraping process, database updates, and scheduling.  
  It communicates directly with PostgreSQL inside the Docker network.

### ⚙️ How It Works Together
1. When Docker starts, it launches both **PostgreSQL** and **Airflow** containers.  
2. Airflow automatically loads the DAG that performs daily scraping from CBAR.az.  
3. The scraped data is saved to the PostgreSQL container.  
4. Power BI then connects to the same database to visualize the data.

### 💡 Benefits
- No local setup or dependency issues  
- Easy to start with a single command (`docker-compose up -d`)  
- Portable — can run on any system with Docker installed  
- Clean and isolated environment for all components  

By containerizing the entire system, this project achieves **full automation, stability, and reproducibility** — ideal for production or cloud deployment.

---

## 🧑‍💻 How to Run Locally
1. Clone this repository  
   ```bash
   git clone https://github.com/yourusername/cbar-currency-pipeline.git
   cd cbar-currency-pipeline

2. Start the system
   ```bash
   docker-compose up -d

4. Open Airflow UI
   ```bash
   Open Airflow UI → http://localhost:8080

5. Connect Power BI to PostgreSQL → Database: currency

