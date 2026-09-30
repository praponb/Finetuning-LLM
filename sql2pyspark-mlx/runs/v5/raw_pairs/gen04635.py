from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 66)
    .groupBy("carrier", "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
    )
    .filter(F.col("pct_ups") >= 75)
    .orderBy("carrier")
)
