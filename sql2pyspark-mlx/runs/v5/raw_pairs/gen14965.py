from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .withColumn("status_lower", F.lower("status"))
    .withColumn("carrier_prefix", F.substring("carrier", 1, 3))
)

result = (
    valid
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .rollup(F.col("o.channel").alias("channel"), F.col("src.carrier").alias("carrier"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.cost")).alias("avg_cost"),
    )
    .orderBy(F.col("channel").desc())
)
