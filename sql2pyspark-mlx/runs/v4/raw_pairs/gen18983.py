from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity_bucket").desc_nulls_last())
w4 = Window.orderBy("quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id", "product_id")
w_running = Window.partitionBy("order_id")
w7 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w8 = Window.partitionBy("order_id").orderBy("order_id_sum_quantity", "id")
w9 = Window.partitionBy("product_id", "order_id")

metrics = (
    df_order_items
    .select(
        "*",
        F.ntile(4).over(w).alias("quantity_bucket"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
    )
)

enriched = (
    metrics
    .withColumn("quantity_bucket_dense_rank", F.dense_rank().over(w3))
    .withColumn("last_known_last_known_quantity", F.last("last_known_quantity", ignorenulls=True).over(w4))
    .withColumn("running_quantity_bucket", F.sum("quantity_bucket").over(w2))
    .withColumn("order_id_sum_quantity", F.sum("quantity").over(w5))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w_running))
)

enriched_order_items = (
    enriched
    .select(
        "*",
        F.max("last_known_quantity").over(w_running).alias("order_id_max_last_known_quantity"),
        F.row_number().over(w7).alias("seq_no"),
        F.min("order_id_sum_quantity").over(w_running).alias("order_id_min_order_id_sum_quantity"),
        F.ntile(4).over(w8).alias("order_id_sum_quantity_bucket"),
        F.count("quantity_bucket_dense_rank").over(w9).alias("product_id_count_quantity_bucket_dense_rank"),
    )
)

filtered = (
    enriched_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("order_id_sum_quantity_num", F.col("order_id_sum_quantity").cast("string"))
)

grouped = (
    filtered
    .withColumn("product_id_count_quantity_bucket_dense_rank_tier", F.when(F.col("product_id_count_quantity_bucket_dense_rank") >= 13, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quantity_bucket_dense_rank_tier", F.when(F.col("quantity_bucket_dense_rank") >= 9, "HIGH").otherwise("LOW"))
    .cube(
        "product_id",
        "product_id_count_quantity_bucket_dense_rank_tier",
        "quantity_bucket_dense_rank_tier",
    )
    .agg(
        F.percentile_approx("last_known_last_known_quantity", [0.25, 0.5, 0.75]).alias("last_known_last_known_quantity_quartiles"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("quantity_bit_and") > 1)
)

result = (
    grouped
    .select(
        "product_id",
        "product_id_count_quantity_bucket_dense_rank_tier",
        "quantity_bucket_dense_rank_tier",
        "last_known_last_known_quantity_quartiles",
        "quantity_bit_and",
        "gid",
    )
    .orderBy("quantity_bit_and", "gid")
)
