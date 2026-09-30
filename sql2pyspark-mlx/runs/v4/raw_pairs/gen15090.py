from pyspark.sql import functions as F

base = df_shipments.filter(F.col("order_id").isNotNull())

summary = (
    base
    .groupBy("status")
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
    )
)

result = summary
