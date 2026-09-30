from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 2200)
    .select("sale_date")
    .union(df_customers.select("signup_date"))
    .orderBy("sale_date")
)
