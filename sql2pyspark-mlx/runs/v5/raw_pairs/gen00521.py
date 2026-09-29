from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w5 = Window.partitionBy("order_id").orderBy(F.col("order_id_count_unit_price").desc())
w6 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_grp = Window.partitionBy("product_id", "order_id").orderBy(F.col("product_id_max_quantity").desc(), "id")
w_running = Window.partitionBy("order_id", "product_id")
w_part = Window.orderBy("product_id_max_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_first(), "id")

scored = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.ntile(3).over(w).alias("unit_price_bucket"),
        F.max("quantity").over(w2).alias("product_id_max_quantity"),
        F.count("unit_price").over(w3).alias("order_id_count_unit_price"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.price"))
)

metrics = (
    enriched
    .withColumn("order_id_count_unit_price_percent_rank", F.percent_rank().over(w5))
    .withColumn("unit_price_bucket_2", F.ntile(10).over(w6))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("order_id_count_price", F.count("price").over(w_running))
    .withColumn("first_known_product_id_max_quantity", F.first("product_id_max_quantity", ignorenulls=True).over(w_part))
)

deduped = (
    metrics
    .withColumn("row_num_2", F.row_number().over(w10))
    .filter(F.col("row_num_2") == 1)
)

result = (
    deduped
    .groupBy("order_id")
    .agg(
        F.percentile_approx("unit_price_bucket", [0.25, 0.5, 0.75]).alias("unit_price_bucket_quartiles"),
        F.min("product_id_max_quantity").alias("min_product_id_max_quantity"),
        F.round(F.avg("unit_price_bucket_2"), 2).alias("avg_unit_price_bucket_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("product_id_max_quantity", 0.5).alias("product_id_max_quantity_p50"),
    )
    .orderBy(F.col("quantity_bit_and").desc_nulls_last())
)
