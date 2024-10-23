import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
import duckdb
import os
import time
import openpyxl
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

st.set_page_config(layout="wide")

# Cache the function to load data
@st.cache_data
def load_data(file_path):
    file_extension = os.path.splitext(file_path)[1]
    
    # Load the data based on file extension
    if file_extension == '.csv':
        data = pd.read_csv(
            file_path,
            delimiter=';',               
            on_bad_lines='skip',          
            low_memory=False,             
            na_values=['NULL', ' ']       
        )
    elif file_extension == '.parquet':
        data = pd.read_parquet(file_path)
    else:
        raise ValueError("Unsupported file format")
    
    return data

# Load the data using DuckDB for fast retrieval
@st.cache_data
def query_data_with_duckdb(query, parquet_files):
    conn = duckdb.connect(database=':memory:')
    result = conn.execute(query).df()
    conn.close()
    return result

# Load postal code data from Excel
@st.cache_data
def load_postal_code_data(file_path):
    return pd.read_excel(file_path)

# Load the datasets
missed_sales_file = './data/MissedSalesTabel_filtered_labeled.parquet'
customer_file = './data/Customer.parquet'
sales_file = './data/SaleTabel_labeled.parquet'
postal_code_file = './data/CustomerCityPC.xlsx'

missed_sales = load_data(missed_sales_file)
customer_data = load_data(customer_file)
actual_sales = load_data(sales_file)
postal_code_data = load_postal_code_data(postal_code_file)


# Page title
st.title("Missed Sales Dashboard")

# Convert Date columns in both Missed Sales and Actual Sales data to datetime format
missed_sales['Date'] = pd.to_datetime(missed_sales['Date'], errors='coerce')
actual_sales['Date'] = pd.to_datetime(actual_sales['Date'], errors='coerce')

# Filter the missed sales data where GEMISTEVERKOOPFLG equals 1 (Missed Sales flag)
missed_sales_filtered = missed_sales[missed_sales['GEMISTEVERKOOPFLG'] == 1].copy()

# Translation dictionary for reasons (Dutch to English)
reason_translation = {
    "Product niet verkrijgbaar bij leverancier": "Product not available from supplier",
    "Product ontbreekt hier, maar nog aanwezig in andere eenheid (niet doorbesteld)": "Product missing here but present in another unit (not reordered)",
    "aantal aangepast omwille van quota": "quantity adjusted due to quota",
    "dels doorbesteld": "dels reordered",
    "doorbesteld": "reordered",
    "geen prodcode SH,SD,SV,CH, CV voor webshop": "no prodcode SH,SD,SV,CH, CV for webshop",
    "klanten met leveringsplicht enkel prodcode SH,SD,SV,CH, CV": "customers with delivery obligation only prodcode SH,SD,SV,CH, CV",
    "product geblokkeerd voor deze klant /selective distributie": "product blocked for this customer / selective distribution",
    "product is vervangen": "product has been replaced",
    "product mag bidir niet besteld worden of enkel TO": "product may not be ordered bidir or only TO",
    "stock 0": "stock 0",
    "stock 0 omwille van quota": "stock 0 due to quota",
    "substitutieproduct zal geleverd worden": "substitute product will be delivered",
    "vervangingsproduct zal geleverd worden": "replacement product will be delivered",
    "Product is uit de handel (stocktoeak = UH)": "Product is out of business (stocktoeak = UH)",
    "code niet doorbestellen stat op bij product": "code do not reorder listed on product",
    "fout bij read": "error during read",
    "product op vloer dus niet doorbestellen": "product on floor so do not reorder",
    "niet doorbestellen dus aantal aangepast": "do not reorder, so quantity adjusted",
    "tot lijnen > 99999": "up to lines > 99999"
}

# Translate 'Reason' column to English
missed_sales_filtered['Reason'] = missed_sales_filtered['Reason'].map(reason_translation)

############################
# Line Chart: Missed Sales Over Time by Reason
############################

# Drop rows where Date or Reason is missing
missed_sales_clean = missed_sales_filtered.dropna(subset=['Date', 'Reason']).copy()

# Convert 'Reason' column to string type to avoid category-related issues
missed_sales_clean['Reason'] = missed_sales_clean['Reason'].astype(str)

# Convert 'AantalBesteld' to numeric (int or float) so it can be summed
missed_sales_clean['AantalBesteld'] = pd.to_numeric(missed_sales_clean['AantalBesteld'], errors='coerce')

# Create a new 'DatePeriod' column for grouping purposes
missed_sales_clean['DatePeriod'] = missed_sales_clean['Date'].dt.to_period('M')

# Group data by DatePeriod and Reason, summing 'AantalBesteld'
missed_sales_by_time_reason = (
    missed_sales_clean
    .groupby(['DatePeriod', 'Reason'], as_index=False)
    .sum(numeric_only=True)
)

# Convert 'DatePeriod' to a timestamp for easier visualization
missed_sales_by_time_reason['Date'] = missed_sales_by_time_reason['DatePeriod'].dt.to_timestamp()

# Create a wider line chart with Plotly
fig_line = px.line(missed_sales_by_time_reason, 
                   x='Date', 
                   y='AantalBesteld', 
                   color='Reason',
                   labels={'Date': 'Month and Year', 'AantalBesteld': 'Missed Sales Quantity', 'Reason': 'Reason'},
                   title="Missed Sales Over Time by Reason")

# Customize x-axis formatting and legend
fig_line.update_xaxes(tickformat="%b %y", tickangle=45)
fig_line.update_layout(
    width=1000,  # Adjust width
    height=600,  # Adjust height
    legend=dict(
        orientation="v",  # Vertical legend
        yanchor="top",
        y=1,
        xanchor="left",
        x=1.05
    )
)

# Display the line chart in Streamlit
st.plotly_chart(fig_line)

# Merge the Missed Sales and Customer data on 'CustomerNbr' (common key)
missed_sales_merged = pd.merge(missed_sales_clean, customer_data, on='CustomerNbr', how='inner')

# Merge the Actual Sales and Customer data on 'CustomerNbr' (common key)
actual_sales_merged = pd.merge(actual_sales, customer_data, on='CustomerNbr', how='inner')

# Translation dictionary for customer types
customer_type_translations = {
    'Diversen': 'Various',
    'Kliniek': 'Clinic',
    'Lid': 'Member',
    'Leveringsplicht': 'Delivery Duty',
    'Niet lid': 'Non-member',
    'Personeel': 'Staff',
    'Speciale klanten EXPORT': 'Special Customers EXPORT',
    'Transfer Orders': 'Transfer Orders',
    'Vergunninghouder': 'Permit Holder',
    'Webshop': 'Webshop'
}

# Translate customer types in both datasets using the dictionary
missed_sales_merged['CustomerTypeTranslated'] = missed_sales_merged['CustomerType'].replace(customer_type_translations)
actual_sales_merged['CustomerTypeTranslated'] = actual_sales_merged['CustomerType'].replace(customer_type_translations)

############################
# Bar Chart: Missed Sales and Actual Sales by Customer Type
############################

# Group missed sales and actual sales by customer type
missed_sales_by_customer_type = missed_sales_merged.groupby('CustomerTypeTranslated')['AantalBesteld'].sum().reset_index(name='TotalMissedSales')
actual_sales_by_customer_type = actual_sales_merged.groupby('CustomerTypeTranslated')['SaleQuantity'].sum().reset_index(name='TotalSales')

st.subheader("Total Missed Sales vs Total Sales by Customer Type")

# Create side-by-side bar charts for missed sales and actual sales by customer type
col1, col2 = st.columns(2)

with col1:
    fig_missed_sales = px.bar(missed_sales_by_customer_type, 
                              x='CustomerTypeTranslated', 
                              y='TotalMissedSales',
                              labels={'CustomerTypeTranslated': 'Customer Type (Translated)', 
                                      'TotalMissedSales': 'Total Missed Sales (AantalBesteld)'},
                              title="Missed Sales by Customer Type",
                              color='TotalMissedSales',
                              color_continuous_scale='Viridis')
    st.plotly_chart(fig_missed_sales)

with col2:
    fig_actual_sales = px.bar(actual_sales_by_customer_type, 
                              x='CustomerTypeTranslated', 
                              y='TotalSales',
                              labels={'CustomerTypeTranslated': 'Customer Type (Translated)', 
                                      'TotalSales': 'Total Actual Sales (SaleQuantity)'},
                              title="Actual Sales by Customer Type",
                              color='TotalSales',
                              color_continuous_scale='Blues')
    st.plotly_chart(fig_actual_sales)

############################
# Heatmap: Missed Sales and Actual Sales by Customer City
############################

# Group missed sales and actual sales by customer city
missed_sales_by_city = missed_sales_merged.groupby('CustomerCity')['AantalBesteld'].sum().reset_index(name='TotalMissedSales')
actual_sales_by_city = actual_sales_merged.groupby('CustomerCity')['SaleQuantity'].sum().reset_index(name='TotalSales')

st.subheader("Missed Sales vs Sales by City (Heatmap)")
# Create side-by-side heatmaps for missed sales and actual sales by city
col3, col4 = st.columns(2)

with col3:
    fig_missed_sales_city = px.bar(missed_sales_by_city, 
                                   x='CustomerCity', 
                                   y='TotalMissedSales', 
                                   color='TotalMissedSales',
                                   title="Total Missed Sales by City",
                                   labels={'CustomerCity': 'City', 'TotalMissedSales': 'Total Missed Sales (AantalBesteld)'},
                                   color_continuous_scale="Viridis")
    st.plotly_chart(fig_missed_sales_city)

with col4:
    fig_actual_sales_city = px.bar(actual_sales_by_city, 
                                   x='CustomerCity', 
                                   y='TotalSales', 
                                   color='TotalSales',
                                   title="Total Actual Sales by City",
                                   labels={'CustomerCity': 'City', 'TotalSales': 'Total Actual Sales (SaleQuantity)'},
                                   color_continuous_scale="Blues")
    st.plotly_chart(fig_actual_sales_city)

############################
# Begin of Script
############################

# Convert CustomerPostalCode and CustomerCity to string to ensure consistency
customer_data['CustomerCity'] = customer_data['CustomerCity'].astype(str)
customer_data['CustomerPostalCode'] = customer_data['CustomerPostalCode'].astype(str)
postal_code_data['CustomerPostalCode'] = postal_code_data['CustomerPostalCode'].astype(str)

# Merge data for missed and actual sales with customer data
missed_sales_query = """
    SELECT ms.CustomerNbr, ms.AantalBesteld, ms.Date, c.CustomerCity, c.CustomerPostalCode
    FROM parquet_scan('""" + missed_sales_file + """') ms
    INNER JOIN parquet_scan('""" + customer_file + """') c
    ON ms.CustomerNbr = c.CustomerNbr
    WHERE ms.GEMISTEVERKOOPFLG = 1
"""

actual_sales_query = """
    SELECT s.CustomerNbr, s.SaleQuantity, s.Date, c.CustomerCity, c.CustomerPostalCode
    FROM parquet_scan('""" + sales_file + """') s
    INNER JOIN parquet_scan('""" + customer_file + """') c
    ON s.CustomerNbr = c.CustomerNbr
"""

missed_sales_merged = query_data_with_duckdb(missed_sales_query, [missed_sales_file, customer_file])
actual_sales_merged = query_data_with_duckdb(actual_sales_query, [sales_file, customer_file])

# Convert CustomerCity and CustomerPostalCode to string to ensure consistency in merged data
missed_sales_merged['CustomerCity'] = missed_sales_merged['CustomerCity'].astype(str)
missed_sales_merged['CustomerPostalCode'] = missed_sales_merged['CustomerPostalCode'].astype(str)
actual_sales_merged['CustomerCity'] = actual_sales_merged['CustomerCity'].astype(str)
actual_sales_merged['CustomerPostalCode'] = actual_sales_merged['CustomerPostalCode'].astype(str)

# Merge postal code data to get Latitude and Longitude
missed_sales_merged = missed_sales_merged.merge(postal_code_data[['CustomerPostalCode', 'Latitude', 'Longitude']], on='CustomerPostalCode', how='left')
actual_sales_merged = actual_sales_merged.merge(postal_code_data[['CustomerPostalCode', 'Latitude', 'Longitude']], on='CustomerPostalCode', how='left')

# Convert Latitude and Longitude back to float to ensure proper plotting
missed_sales_merged['Latitude'] = missed_sales_merged['Latitude'].fillna(0).astype(float)
missed_sales_merged['Longitude'] = missed_sales_merged['Longitude'].fillna(0).astype(float)
actual_sales_merged['Latitude'] = actual_sales_merged['Latitude'].fillna(0).astype(float)
actual_sales_merged['Longitude'] = actual_sales_merged['Longitude'].fillna(0).astype(float)

# Filter out rows with missing geolocation data (latitude and longitude equal to 0)
missed_sales_clean = missed_sales_merged[(missed_sales_merged['Latitude'] != 0) & (missed_sales_merged['Longitude'] != 0)].copy()
actual_sales_clean = actual_sales_merged[(actual_sales_merged['Latitude'] != 0) & (actual_sales_merged['Longitude'] != 0)].copy()

############################
# Map with Missed Sales vs Sales by Customer City
############################

st.subheader("Missed Sales vs Sales by City (2D Map Overlay)")

# Aggregate missed sales and actual sales by CustomerCity, Latitude, Longitude
missed_sales_by_city = missed_sales_clean.groupby(['CustomerCity', 'Latitude', 'Longitude'])['AantalBesteld'].sum().reset_index(name='TotalMissedSales')
actual_sales_by_city = actual_sales_clean.groupby(['CustomerCity', 'Latitude', 'Longitude'])['SaleQuantity'].sum().reset_index(name='TotalSales')

# Ensure marker sizes are non-negative and within a reasonable range
missed_sales_by_city['MarkerSize'] = missed_sales_by_city['TotalMissedSales'].apply(lambda x: max(x / 10000, 1))
actual_sales_by_city['MarkerSize'] = actual_sales_by_city['TotalSales'].apply(lambda x: max(x / 10000, 1))

# Create a 2D scatter map with Plotly
fig = go.Figure()

# Add missed sales as points on the map
fig.add_trace(go.Scattermapbox(
    lon=missed_sales_by_city['Longitude'],
    lat=missed_sales_by_city['Latitude'],
    mode='markers',
    marker=go.scattermapbox.Marker(
        size=missed_sales_by_city['MarkerSize'],  # Ensure size is non-negative
        color='red',
        opacity=0.6
    ),
    text=missed_sales_by_city['CustomerCity'] + ' Missed Sales: ' + missed_sales_by_city['TotalMissedSales'].astype(str),
    name='Missed Sales'
))

# Add actual sales as points on the map
fig.add_trace(go.Scattermapbox(
    lon=actual_sales_by_city['Longitude'],
    lat=actual_sales_by_city['Latitude'],
    mode='markers',
    marker=go.scattermapbox.Marker(
        size=actual_sales_by_city['MarkerSize'],  # Ensure size is non-negative
        color='blue',
        opacity=0.6
    ),
    text=actual_sales_by_city['CustomerCity'] + ' Actual Sales: ' + actual_sales_by_city['TotalSales'].astype(str),
    name='Actual Sales'
))

# Update layout to use OpenStreetMap
fig.update_layout(
    mapbox=dict(
        accesstoken='pk.eyJ1IjoibWluZHRyaWMiLCJhIjoiY20ya2Fydmh4MGM1ZjJqcjEycXNhMjR5aSJ9.yDnFvogUha5P5-C6J12BtQ',  # Replace with a valid Mapbox token
        style='open-street-map',
        center=dict(lon=4.469936, lat=50.503887),  # Centered on Belgium
        zoom=6
    ),
    margin={'l': 0, 'r': 0, 't': 0, 'b': 0},
    title="Missed Sales vs Actual Sales in Belgium (2D View with OpenStreetMap)",
    showlegend=True
)

# Display the map
st.plotly_chart(fig)

############################
# End of Script
############################
