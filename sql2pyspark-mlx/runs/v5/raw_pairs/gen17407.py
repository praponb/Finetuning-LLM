from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-04-30", "2024-05-01") & (F.col("shipped_date") >= "2025-03-04"))
    .groupBy("carrier")
    .agg(
        F.count("cost").alias("cost_count"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
    )
    .filter(F.col("pct_fedex") >= 34)
)
