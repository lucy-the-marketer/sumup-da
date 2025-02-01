# SUMUP-DA

SUMUP-DA is a **dbt project** designed to **combine web orders and lead funnel data** for the POS Lite team. The goal is to **enable self-service analytics** by providing clean, structured, and transformed data marts.

---

## 📌 Background

**POS Lite** operates on a **short two-week sales cycle** and sells through two channels:

1. **Website Orders** – Customers visit the site via marketing campaigns and place orders directly.
2. **Contact Sales Form** – Customers submit a contact form (stored in Salesforce). A Lead Development Representative (LDR) then follows up, identifies customer needs, and closes deals.

---

## ❓ Problem Statement

The **POS Lite Team Lead** requires a **consolidated view** of both **web_orders** and **leads_funnel** data to effectively track and optimize performance across sales channels.

---

## 🔎 Assumptions

- The team exclusively handles the **POS Lite** product.
- The project covers the following countries: **IE, ES, GB, CH** (as available in the database).
- An order is considered valid only if `NB_OF_POSLITE_ITEMS_ORDERED` is greater than 1; a minimum of one item must be ordered to complete an order.
- Leads and fake leads are aggregated on the same day.  
  _Assumption:_ Fake leads are not identified from the previous day’s leads, so the daily count of fake leads should never exceed the count of total leads.

---

## ⚠️ Discovered Data Issues & Cleaning Solutions

During data analysis, several issues were identified. Below is a summary of these issues along with the solutions implemented:

| **Issue**                                    | **Description**                                                                 | **Solution**                                                                                                   |
|----------------------------------------------|---------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| **1️⃣ Campaign ID as Float Values**         | Some `campaign_id` values appear as floats rather than integers.                | Applied a `clean_id()` macro to enforce integer formatting.                                                   |
| **2️⃣ Invalid String Values in Integer Fields** | Integer columns contained non-numeric values (e.g., `"f"`).                     | Utilized an `intify()` macro to remove non-numeric values and enforce correct data types.                       |
| **3️⃣ Inconsistent NULL vs. Zero Values**     | Integer fields inconsistently used `NULL` and `0` values.                       | Standardized missing value treatment using the `intify()` macro.                                                |
| **4️⃣ Inconsistent Channel Naming**           | Variations such as `search -shopping` vs. `search-shopping` were observed.        | Applied a `normalize_text()` macro to ensure uniform channel naming.                                            |
| **5️⃣ Data Outliers**                         | Daily lead/order counts exceeding 100 were flagged as abnormal. For other metrics, values exceeding 1.5 times the average were considered outliers. Cost metrics are handled separately. | Outliers in counts are set to 0, and other metric outliers are replaced with the average value.                   |
| **6️⃣ Missing Date/Country_ID Values**        | Some records were missing `date` or `country_id`.                               | When possible, these values are mapped from related datasets via `campaign_id`; otherwise, they are assigned a default value (`1999-01-01`) or labeled as `unknown`. |

---

## 🛠 Data Pipeline Breakdown

### 1️⃣ Base Layer (`models/base/`)
- **Purpose:** Contains raw tables that replicate the source databases.
- **Characteristics:**  
  - No transformations are applied here.  
  - Data is ingested as-is from the source systems.

### 2️⃣ Staging Layer (`models/staging/`)
- **Purpose:** Cleans and standardizes the raw data.
- **Processes:**  
  - Applies SQL best practices and transformation macros (e.g., `clean_id()`, `intify()`, `normalize_text()`) to ensure data quality and consistency.

### 3️⃣ Marts Layer (`models/marts/`)
- **Purpose:** Provides fact and dimension tables that support self-service analytics and dashboarding.
- **Key Integration:**  
  - Combines data from **web_orders** and **leads_funnel** to create consolidated views for performance tracking.

---

## 🚀 Current Limitations & Next Steps

- **Data Seeding Issues:**  
  - The dbt seed process encountered challenges due to an excess of variables. As a workaround, tables were created in `my_database.db` and data was imported directly.

- **Outlier Detection:**  
  - Outlier detection is currently implemented in a hard-coded manner. Future improvements could include a dynamic, configurable approach to handle outliers more effectively.

- **Mapping Missing Values:**  
  - When `date` or `country_id` is missing, a mapping is attempted using `campaign_id` from other datasets. If no mapping is available, a default value (e.g., `1999-01-01`) or the label `unknown` is used.
