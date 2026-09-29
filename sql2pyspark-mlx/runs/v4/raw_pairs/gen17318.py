from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc())
)
