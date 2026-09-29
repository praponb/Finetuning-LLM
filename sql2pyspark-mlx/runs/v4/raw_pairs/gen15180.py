from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("carrier") == "Flash")

agg = (
    recent_shipments
    .groupBy("carrier", "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
    )
)

result = agg.select("carrier", "status", "pct_ups")
