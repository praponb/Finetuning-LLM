from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-04-21", "2025-03-10") & F.col("order_id").isin([25, 27, 30, 32]))
    .withColumn("cost_whole", F.ceil("cost"))
)

summary = (
    src
    .groupBy(F.month("delivered_date").alias("delivered_month"), "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
    )
)

result = (
    summary
    .select("delivered_month", "status", "pct_returned")
    .orderBy("status", F.col("delivered_month").desc())
)
