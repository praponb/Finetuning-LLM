from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") >= "2023-04-01")
    .select("city")
    .subtract(df_stores.filter(F.length("store_name") > 14).select("city"))
)
