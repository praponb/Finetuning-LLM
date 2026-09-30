from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("order_id").isin([5, 7, 20, 23]) & (F.col("carrier") == "Kerry"))
    .withColumn("delivered_date_plus_71d", F.date_add("delivered_date", 71))
)

summary = (
    recent_shipments
    .groupBy("order_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = summary.filter(F.col("stddev_cost") > 59)
