from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("product_id")
w4 = Window.orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("least_unit_price_quantity", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("order_id")
w7 = Window.partitionBy("order_id").orderBy(F.col("row_seq_2").asc_nulls_last(), "id")
w8 = Window.partitionBy("product_id").orderBy(F.col("quantity_avg_2").desc_nulls_last(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") >= 300))
    .withColumn("least_unit_price_quantity", F.least("unit_price", "quantity"))
)

cleaned = (
    filtered
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_minus_least_unit_price_quantity", (F.col("quantity") - F.col("least_unit_price_quantity")))
)

metrics = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("row_seq_2", F.row_number().over(w2))
    .withColumn("product_id_min_quantity_minus_least_unit_price_quantity", F.min("quantity_minus_least_unit_price_quantity").over(w3))
)

with_windows = (
    metrics
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "row_seq",
        "row_seq_2",
        F.last("product_id_min_quantity_minus_least_unit_price_quantity", ignorenulls=True).over(w4).alias("last_known_product_id_min_quantity_minus_least_unit_price_quantity"),
        F.avg("quantity").over(w5).alias("quantity_avg_2"),
        F.corr("quantity", "unit_price").over(w6).alias("corr_quantity_unit_price"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

latest_order_items = (
    latest
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest_order_items
    .groupBy("product_id")
    .agg(
        F.min("corr_quantity_unit_price").alias("min_corr_quantity_unit_price"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.percentile_approx("row_seq_2", 0.9).alias("row_seq_2_p90"),
    )
    .filter(F.col("avg_unit_price") > 0)
    .orderBy(F.col("avg_unit_price").desc_nulls_last())
)
