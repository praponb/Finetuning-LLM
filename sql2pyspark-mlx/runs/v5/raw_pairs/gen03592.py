from pyspark.sql import functions as F

result = (
    df_inventory
    .select(F.col("last_restock").alias("event_date"))
    .unionByName(df_customers.select(F.col("signup_date").alias("event_date")))
    .orderBy("event_date")
)
