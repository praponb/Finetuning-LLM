from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())

filtered = (
    df_order_items
    .filter(F.col("quantity") <= 6)
    .select(
        "*",
        F.when(F.col("unit_price") >= 360, "High").when(F.col("unit_price") >= 140, "Medium").otherwise("Low").alias("unit_price_band"),
    )
)

enriched = (
    filtered
    .withColumn("cumulative_quantity", F.sum("quantity").over(w))
    .withColumn("unit_price_dense_rank", F.dense_rank().over(w_recent))
)

cleaned = (
    enriched
    .filter(F.col("unit_price_dense_rank") <= 7)
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
)

filtered_order_items = (
    cleaned
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_dense_rank_tier", F.when(F.col("unit_price_dense_rank") >= 8, "High").when(F.col("unit_price_dense_rank") >= 3, "Medium").otherwise("Low"))
    .withColumn("unit_price_int_times_quantity", (F.col("unit_price_int") * F.col("quantity")))
)

result = (
    filtered_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.avg("unit_price_int_times_quantity"), 2).alias("avg_unit_price_int_times_quantity"),
        F.sum("unit_price_int_times_quantity").alias("total_unit_price_int_times_quantity"),
        F.round(F.avg("unit_price_dense_rank"), 2).alias("avg_unit_price_dense_rank"),
        F.sum("unit_price_int_times_quantity").alias("total_unit_price_int_times_quantity_2"),
    )
    .orderBy(F.col("total_unit_price_int_times_quantity_2").desc())
)
