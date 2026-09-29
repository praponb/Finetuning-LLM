from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("quantity") != 3)
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 1.07), 2))
)

order_id_summary = (
    recent_order_items
    .groupBy("order_id")
    .agg(F.count("*").alias("cnt"))
)

result = order_id_summary.select("order_id", "cnt")
