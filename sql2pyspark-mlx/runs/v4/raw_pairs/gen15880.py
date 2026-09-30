from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .union(df_store_sales.filter(F.col("units") == 32).select("sale_date"))
    .distinct()
)
