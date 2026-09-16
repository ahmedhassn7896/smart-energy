# Power BI Setup & Dashboard Guide: Smart Energy Consumption Analytics

This guide provides step-by-step instructions to connect **Microsoft Power BI Desktop** to the real-time **ClickHouse** analytical database and build the dashboard.

---

## 1. Connection Methods

### Method A: ClickHouse ODBC Driver (Recommended for Live DirectQuery)
1. Download and install the **ClickHouse ODBC Driver** (x64) from GitHub / ClickHouse Releases.
2. Open **ODBC Data Source Administrator (64-bit)** in Windows.
3. Under System DSN, click **Add**, select `ClickHouse Unicode DSN (ANSI or Unicode)`:
   - **Host / IP**: `localhost`
   - **Port**: `8123` (HTTP) or `9000` (Native)
   - **Database**: `analytics`
   - **User**: `default`
   - **Password**: `clickhouse`
4. In Power BI Desktop:
   - Click **Get Data** > **ODBC** > Select your DSN.
   - Choose **DirectQuery** (or Import) and select the tables:
     - `daily_energy_consumption`
     - `hourly_energy_consumption`
     - `monthly_energy_consumption`
     - `household_energy_summary`
     - `energy_anomalies`
     - `energy_forecast`

### Method B: Power BI Web / REST API Connector
In Power BI Desktop:
1. Click **Get Data** > **Web**.
2. URL: `http://localhost:8123/?query=SELECT+*+FROM+analytics.daily_energy_consumption+FORMAT+JSONEachRow`
3. Expand records into columns.

---

## 2. Dashboard Layout & Visualizations

### 📊 Page 1: Executive Overview & KPI Dashboard
- **Header KPI Cards**:
  - **Total Consumption (kWh)**: `Total Consumption (kWh)` measure
  - **Avg Daily Consumption (kWh)**: `Avg Daily Consumption (kWh)`
  - **Total Households**: `Total Households` (e.g. 5,567 households)
  - **Total Meter Readings**: `Total Readings`
  - **Peak Consumption Day**: `Peak Daily Consumption`
  - **Detected Anomalies**: `Total Anomalies`
- **Visuals**:
  - **Consumption Over Time**: Line chart of `reading_date` vs `total_kwh` with zoom slider.
  - **Top 10 Energy Consumers**: Clustered bar chart of `lcl_id` vs `total_kwh` (Filtered to Top 10).
  - **Consumer Tier Distribution**: Donut chart of `consumer_tier` (High, Medium, Low) by household count.
- **Slicers**:
  - `Year` / `Month` slider
  - `Consumer Tier` dropdown
  - `Household ID (lcl_id)` search box

### ⏱️ Page 2: Temporal & Peak Usage Patterns
- **Visuals**:
  - **Hourly Consumption Profile**: Line/Area chart comparing Weekday vs Weekend average hourly kWh (`hour` on X-axis, `avg_hourly_kwh` on Y-axis, `is_weekend` as Legend).
  - **Seasonal & Monthly Consumption**: Column chart of `month_name` vs `total_kwh` grouped by `season`.
  - **Peak Hour Distribution**: Heatmap / Matrix showing peak consumption hour across days of week.

### 🚨 Page 3: Anomaly Detection & Grid Reliability
- **Visuals**:
  - **Anomaly Severity Breakdown**: Donut chart of `severity` (`Critical`, `High`, `Warning`).
  - **Anomaly Timeline**: Scatter plot of `reading_datetime` vs `kwh` colored by `severity` with baseline reference line (`expected_kwh`).
  - **Anomalous Households Table**: Detailed table listing `lcl_id`, `reading_datetime`, `kwh`, `expected_kwh`, `z_score`, `severity`.

### 📈 Page 4: Demand Forecasting & Predictive Planning
- **Visuals**:
  - **Actual vs 30-Day Demand Forecast**: Time-series line chart with `forecast_date` on X-axis:
    - Solid Line: `actual_kwh` (Historical)
    - Dashed Line: `forecast_kwh` (Predicted 30 days)
    - Shaded Area: `lower_bound` to `upper_bound` (95% Confidence Interval)
  - **Forecast Summary KPIs**: Predicted next 30-day aggregate energy demand.
