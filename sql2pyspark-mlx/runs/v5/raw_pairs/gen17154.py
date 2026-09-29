from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("cost") > 32)

summary = (
    filtered_shipments
    .groupBy("carrier")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

result = (
    summary
    .filter(F.col("stddev_cost") < 34)
    .select("carrier", "stddev_cost", "cost_range")
    .orderBy("cost_range", "carrier")
)
