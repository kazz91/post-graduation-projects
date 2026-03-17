# 📊 Data Science Post-Graduation Projects

A collection of hands-on projects developed during a **Post-Graduation in Data Science**, covering the full spectrum of the data science workflow — from exploratory analysis to machine learning and financial market predictions.

---

## 🗂️ Projects Overview

### 1. `olist_data_mining.py` — E-commerce Data Mining (Olist)
Distributed data processing with **PySpark** on the [Olist Brazilian E-commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

- Multi-table SQL joins across 7 datasets (orders, sellers, products, payments, customers, geolocation)
- Aggregated revenue, customer count, and seller count by state
- Descriptive statistics exported to CSV

**Stack:** PySpark · PySpark SQL · Google Colab · Google Drive

---

### 2. `statistics_analysis.py` — Statistical Analysis on Diabetes Dataset
Full exploratory data analysis on a diabetes clinical dataset.

- Descriptive statistics: mean, median, mode, quartiles, variance, standard deviation, coefficient of variation
- Outlier detection using the IQR method (Tukey's fences)
- Quartile-based mortality rate classification appended to the DataFrame
- Sampling techniques: simple random sampling (with and without replacement), stratified sampling
- Confidence interval computation for all three cases (known σ, unknown σ with n ≥ 30, t-Student for n < 30)

**Stack:** Pandas · NumPy · Matplotlib · SciPy · Google Colab

---

### 3. `financial_analysis.py` — Brazilian Stock Market Sector Analysis (B3)
Real-time financial analysis of BDR (Brazilian Depositary Receipts) listed on B3.

- Scraped ticker list from InvestNews using `requests` + `pandas.read_html`
- Fetched 10-day price history for all BDR tickers via `yfinance`
- Aggregated open/close prices by market sector using PySpark SQL
- Min-max normalization for fair cross-sector comparison
- Horizontal bar chart ranking sectors by closing price

**Stack:** PySpark · yfinance · Matplotlib · requests · Google Colab

---

### 4. `stock_prediction.py` — Stock Price Prediction with Linear Regression
End-to-end ML pipeline for 30-day stock price forecasting on the top-performing B3 sector.

- Identified the highest-grossing market sector via PySpark SQL
- Downloaded 2-year historical price data for the top 10 tickers
- Built individual Linear Regression models per ticker using scikit-learn
- Predicted closing prices 30 days into the future
- Plotted historical vs. predicted prices per stock and for the aggregated sector

**Stack:** PySpark · yfinance · scikit-learn · Matplotlib · Plotly · NumPy · Google Colab

---

### 5. `data_visualization.py` — Non-Verbal Tourist Behavior Analysis
Data wrangling and visualization on a non-verbal communication dataset from international tourists.

- Cleaned missing values (`?` → `NaN`)
- Translated and standardized all column names and categorical values to Portuguese
- Proxemics zones mapped to descriptive labels (Intimate / Personal / Social / Public)
- Exported cleaned DataFrame to Excel

**Stack:** Pandas · Google Colab

---

## 🛠️ Technologies Used

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-F7931E?style=flat&logo=scikit-learn&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat&logo=pandas&logoColor=white)
![Google Colab](https://img.shields.io/badge/Google%20Colab-F9AB00?style=flat&logo=googlecolab&logoColor=white)

---

## 🚀 Getting Started

All notebooks were developed in **Google Colab**. To run any script:

1. Open [Google Colab](https://colab.research.google.com/)
2. Upload the `.py` file or paste its contents into a notebook cell
3. Mount your Google Drive if the script reads local datasets
4. Run cells sequentially

> **Note:** Some scripts require datasets stored in Google Drive. Update file paths as needed.

---

## 👤 Author

**Kassio** · [GitHub](https://github.com/kazz91)
