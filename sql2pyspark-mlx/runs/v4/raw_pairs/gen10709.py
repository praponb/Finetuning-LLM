from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_flash"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
    )
    .filter(F.col("pct_flash") < 69)
    .orderBy(F.col("pct_flash").desc())
)
