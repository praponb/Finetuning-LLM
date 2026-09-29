from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter((F.col("status") != "RETURNED") | F.col("cost").between(72, 86))
    .withColumn("cost_band", F.when(F.col("cost") >= 91, "High").when(F.col("cost") >= 33, "Medium").otherwise("Low"))
)

summary = (
    recent_shipments
    .groupBy(
        F.when(F.col("cost") >= 87, "high").when(F.col("cost") >= 34, "mid").otherwise("low").alias("cost_bucket"),
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    summary
    .filter(F.col("avg_cost") > 74)
    .select("cost_bucket", "delivered_month")
    .orderBy(F.col("delivered_month").desc())
)
