from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-06-16", "2025-07-01") & (F.col("delivered_date") > "2024-11-20") & F.month("delivered_date").isin([5, 8, 12]))
    .withColumn("delivered_date_label", F.date_format("delivered_date", "yyyy-MM"))
)

shipments_stats = (
    recent_shipments
    .groupBy(
        F.quarter("shipped_date").alias("shipped_quarter"),
        F.date_trunc("MONTH", "shipped_date").alias("shipped_month"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
)

result = (
    shipments_stats
    .filter(F.col("min_cost") < 60)
    .orderBy(F.col("shipped_quarter").desc())
)
