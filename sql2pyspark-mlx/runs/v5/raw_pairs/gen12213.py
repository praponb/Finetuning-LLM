from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2023-04-01")
    .select("end_date")
    .union(df_store_sales.select("sale_date"))
)
