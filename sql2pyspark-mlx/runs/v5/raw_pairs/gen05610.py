from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") != "FedEx")
    .groupBy("carrier")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
    )
    .orderBy("carrier")
)
