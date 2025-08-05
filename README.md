# 🏡 Airbnb Berlin Analytics

A modern data engineering project to analyze Airbnb trends in Berlin using public data sources and production-grade tooling. This project showcases skills in data ingestion, cleaning, transformation, orchestration, and dashboarding.

## 📌 Goals
- Analyze how Airbnb prices, availability, and host behavior change over time
- Explore the influence of weather and proximity to tourist attractions
- Detect pricing anomalies and seasonal demand shifts

## 📊 Data Sources
- Inside Airbnb (Berlin): Listings, Reviews, Calendar
- Open-Meteo API: Hourly & daily weather data
- OpenTripMap API: Tourist attractions with coordinates & categories

## 🛠 Tech Stack
- Apache Spark (PySpark)
- Delta Lake (Medallion architecture)
- Google Cloud Storage (GCS)
- Apache Airflow (Docker)
- Terraform (infra-as-code)
- Great Expectations (data validation)
- Streamlit (dashboards)
- GitHub Actions (CI/CD)

## 📂 Planned Architecture
Raw → Bronze → Silver → Gold → Dashboard


## 🚧 Week 1 Focus
- Ingest Airbnb, weather, and POI data
- Profile and validate datasets
- Confirm alignment with business questions
- Set up local Spark environment (Docker)
- Initialize GitHub repo with basic CI

---

**Status**: In progress 🚀
