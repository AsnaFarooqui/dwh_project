# dwh_project

Public Transport Data Warehouse Project
This project involves building a data warehouse for analyzing public transport operations using Snowflake, with data ingestion and ETL automation managed via Apache Airflow. The dataset simulates real-world operations of a public transport system, helping uncover patterns in commuter behavior, route performance, weather impact, and operational efficiency.

## 📁 Project Structure

**Data Sources:**
- Excel files
- MySQL Database
- MongoDB Database

Data is categorized and cleaned using Python.

**Data Storage:**

Final storage and analysis happens in Snowflake using a star schema.

**ETL Pipeline:**

Managed using Apache Airflow DAGs for loading the data.

**Tools Used:**

Python, Airflow, Snowflake, SQL, Pandas, Ubuntu (WSL)

**Business Domain:**

The domain is urban public transportation, with data capturing daily operations such as:
- Commuter rides
- Ticketing and pricing
- Vehicle and station data
- Weather and events affecting transportation

**Project Statement:**

To develop a cloud-based data warehouse that integrates multiple sources of public transportation data and provides a scalable solution for analyzing key performance indicators (KPIs) such as ride frequency, revenue, peak times, vehicle health, and environmental influences on transport.

**Star Schema Design**
📌 Dimension Tables:
DimCommuter

DimDate

DimTimeSlot

DimRoute

DimStation

DimTicketOffice

DimVehicle

DimTicket

⭐ Fact Table:
FactRide
Contains keys to all dimension tables along with quantitative metrics like ride counts, fare amount, and time duration.
