from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .subtract(df_store_sales.select(F.col("sale_date").alias("start_date")))
    .orderBy("start_date")
)
