from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.col("opened_date").alias("event_date"))
    .unionByName(df_products.select(F.col("launch_date").alias("event_date")))
    .orderBy(F.col("event_date").desc())
)
