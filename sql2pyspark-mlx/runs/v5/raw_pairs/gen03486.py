from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("least_unit_price_quantity", F.least("unit_price", "quantity"))
)

result = (
    cleaned
    .alias("src")
    .join(df_orders.alias("dim"), F.col("src.order_id") == F.col("dim.id"), "inner")
    .cube(F.col("dim.status").alias("status"), F.col("dim.channel").alias("channel"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.unit_price")).alias("min_unit_price"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("status").asc_nulls_first())
)
