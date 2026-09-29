from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([8, 17, 20, 28]))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 8)
    .select("order_id", "stddev_cost", "cnt")
    .orderBy(F.col("cnt").desc(), F.col("order_id").desc())
)
