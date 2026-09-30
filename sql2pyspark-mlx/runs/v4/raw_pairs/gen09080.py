from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .select(F.col("opened_date").alias("event_date"))
    .subtract(df_shipments.select(F.col("shipped_date").alias("event_date")))
)
