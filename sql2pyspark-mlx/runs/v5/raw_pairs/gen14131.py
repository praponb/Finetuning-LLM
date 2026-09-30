from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(~((F.col("shipped_date") > "2025-01-08") & ~F.col("status").isin(["IN_TRANSIT", "RETURNED"])))
    .withColumn("carrier_lower", F.lower("carrier"))
)

shipments_stats = (
    src
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
    )
)

result = shipments_stats.select("status", "pct_ups")
