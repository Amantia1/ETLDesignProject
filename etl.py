#!/usr/bin/env python
# coding: utf-8

# # Extract

# In[1]:


import pandas as pd
import re
from etl_logger import setup_logging

logger = setup_logging()
df = pd.read_csv("C:/Users/Amantia/Downloads/archive/SalesTransaction.csv")



def validate_data(record):
    if not record.get('Country'):
        raise ValueError("Missing required field: Country")
    
    CustomerNo = record.get('CustomerNo')
    if CustomerNo is None or CustomerNo < 0:
        raise ValueError("Invalid CustomerNo: must be a positive integer")
    
   

# In[2]:

def etl_process():
    try:
        
        df = pd.read_csv("C:/Users/Amantia/Downloads/archive/SalesTransaction.csv").to_dict(orient='records')
        logger.info("Data extracted successfully")

        # Validate each record
        for record in df:
            try:
                validate_data(record)
                logger.info(f"Record validated: {record}")
            except ValueError as ve:
                logger.error(f"Data validation error for record {record}: {ve}")
                continue  # Skip this record
             
        
        # Proceed with transformation
        
        logger.info("Data transformed successfully")

        # Simulate loading data into a database
        # load_data_to_db(data)  # Assume this function is defined
        logger.info("Data loaded successfully")
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise  # Reraise to mark the task as failed

if __name__ == "__main__":
    etl_process()


df.head()


# In[3]:


df.shape


# In[4]:


df.info()


# # Transform

# ### Standarize cols

# In[5]:


df = df.rename(columns={"TransactionNo": "transaction_id", "Date": "date", "ProductNo": "product_id",
                        "ProductName": "name", "Price": "price", "Quantity": "quantity", 
                        "CustomerNo": "customer_id", "Country": "country"})


# In[6]:


df.info()


# ### Drop data duplicate

# In[7]:


df.duplicated().sum()


# In[8]:


df.drop_duplicates(keep='first', inplace=True)


# In[9]:


df.duplicated().sum()


# ### Handling Missing Value

# In[10]:


df.isnull().sum()


# In[11]:


df[df['customer_id'].isnull()].head()


# In[12]:


df['customer_id'].fillna(0, inplace=True)


# In[13]:


df.isnull().sum()


# ### Dealing with Unmatched Data

# In[14]:


df.loc[
    df["transaction_id"].str.contains("C")
].shape


# In[15]:


c_trans = df.loc[
    df["transaction_id"].str.contains("C")
].index


# In[16]:


df.drop(c_trans, inplace=True)


# In[17]:


df.loc[
    df["quantity"] <= 0
].shape


# In[18]:


df.shape


# ### Data Type Casting

# In[19]:


df = df.astype({"transaction_id": int, "customer_id": int})
df["date"]= pd.to_datetime(df["date"], format="%m/%d/%Y")


# In[20]:


df.dtypes


# ### Do price fluctuate?

# In[21]:


product_unique = df.loc[
    ~df[["product_id", "name", "price"]].duplicated()
]


# In[22]:


product_unique.sort_values(by=["name", "date"]).head()


# In[23]:


df[["date","product_id", "name", "price"]].sort_values(by=["name", "date"]).head()


# ### Sum the quantity if identical transactions occur

# In[24]:


type(df.groupby(["transaction_id", "date", "product_id", "name", "price", "customer_id", "country"])["quantity"])


# In[25]:


df.loc[
    (df["transaction_id"] == 579171) &
    (df["product_id"] == "22897")
]


# In[26]:


df = df.groupby(["transaction_id", "date", "product_id", "name", "price", "customer_id", "country"])["quantity"].sum().reset_index()


# In[27]:


df.loc[
    (df["transaction_id"] == 579171) &
    (df["product_id"] == "22897")
]


# ### Enriching Data

# In[28]:


df["year"] = df['date'].dt.year
df['quarter'] = df['date'].dt.quarter
df['month'] = df['date'].dt.month
df['week'] = df['date'].dt.isocalendar().week
df["day"] = df['date'].dt.day
df["day_name"] = df['date'].dt.day_name()


# In[29]:


country_id = df["country"].str.upper().str.slice(stop=3) + df["country"].str.len().astype(str)
df["country_id"] = country_id


# In[30]:


df.dtypes


# # Load

# In[31]:


from sqlalchemy import create_engine, text


# In[32]:


engine = create_engine("postgresql://postgres:admin@localhost:5432/AMI")
connection = engine.raw_connection()


# ### Generate Keys for all dimensional tables

# In[33]:


df_customer = df[["customer_id"]].drop_duplicates().sort_values(by="customer_id")
df_customer = df_customer.reset_index(drop=True)
df_customer["customer_key"] = df_customer.index
df_customer.info()


# In[34]:


df_transaction = df[["transaction_id"]].drop_duplicates().sort_values(by="transaction_id")
df_transaction = df_transaction.reset_index(drop=True)
df_transaction["transaction_key"] = df_transaction.index
df_transaction.info()


# In[35]:


df_date = df[["date", "year", "quarter", "month", "week", "day", "day_name"]].drop_duplicates().sort_values(by="date")
df_date = df_date.reset_index(drop=True)
df_date["date_key"] = df_date.index
df_date.info()


# In[36]:


df_country = df[["country", "country_id"] ].drop_duplicates().sort_values(by="country_id")
df_country = df_country.reset_index(drop=True)
df_country["country_key"] = df_country.index
df_country.info()


# In[37]:


df_product = df[['product_id', "name", "price"]]
unique_product = df_product.drop_duplicates().sort_values(by=["name", "price"])
unique_product = unique_product.reset_index(drop=True)
unique_product["product_key"] = unique_product.index

df_unique_product = pd.merge(df_product, unique_product, on=["product_id", "name", "price"]).sort_values(by=["name", "price"]).drop_duplicates()
df_unique_product.info()


# ### Join all to fact table

# In[38]:


result = pd.merge(df, df_customer, on="customer_id")
result = pd.merge(result, df_transaction, on="transaction_id")
result = pd.merge(result, df_date, on=["date", "year", "quarter", "month", "week", "day", "day_name"])
result = pd.merge(result,df_country, on=["country_id", "country"])
result = pd.merge(result, df_unique_product, on=["product_id", "name", "price"])


# In[39]:


result.info()


# ### Add col and Load dim table to postgres

# In[40]:


df_customer.info()


# In[41]:


df_customer_dim = df_customer.set_index("customer_key")
df_customer_dim.to_sql("customer_dim", con=engine, if_exists="replace")
with engine.connect() as conn:
    conn.execute(text("ALTER TABLE customer_dim ADD PRIMARY KEY (customer_key);"))
df_customer_dim.shape


# In[42]:


df_transaction.info()


# In[43]:


df_transaction_dim = df_transaction.set_index("transaction_key")
df_transaction_dim.to_sql("transaction_dim", con=engine, if_exists="replace")
with engine.connect() as conn:
    conn.execute(text("ALTER TABLE transaction_dim ADD PRIMARY KEY (transaction_key);"))
df_transaction_dim.shape


# In[44]:


df_date.info()


# In[45]:


df_date_dim = df_date.set_index("date_key")
df_date_dim['date'] = df_date_dim['date'].dt.date
df_date_dim.to_sql("date_dim", con=engine, if_exists="replace")
with engine.connect() as conn:
    conn.execute(text("ALTER TABLE date_dim ADD PRIMARY KEY (date_key);"))
df_date_dim.shape


# In[46]:


df_unique_product.info()


# In[47]:


df_product_dim = df_unique_product.set_index("product_key")
df_product_dim.to_sql("product_dim", con=engine, if_exists="replace")
with engine.connect() as conn:
    conn.execute(text("ALTER TABLE product_dim ADD PRIMARY KEY (product_key);"))
df_product_dim.shape


# In[48]:


df_country.info()


# In[49]:


df_country_dim = df_country.set_index("country_key")
df_country_dim.to_sql("country_dim", con=engine, if_exists="replace")
with engine.connect() as conn:
    conn.execute(text("ALTER TABLE country_dim ADD PRIMARY KEY (country_key);"))
df_country_dim.shape


# ### Remove col and Load fact table to postgres

# In[50]:


result.info()


# In[51]:


df_sales_fact = result[["customer_key", "transaction_key", "date_key", "product_key", "country_key", "quantity"]].sort_values(by="date_key")
df_sales_fact.to_sql("sales_fact", con=engine, if_exists="replace", index=False)

