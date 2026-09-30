from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("shipped_date").alias("shipped_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_flash"),
    )
    .filter(F.col("pct_flash") > 55)
    .orderBy(F.col("pct_flash").desc())
)
