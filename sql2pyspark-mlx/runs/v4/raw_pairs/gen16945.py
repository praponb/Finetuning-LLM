from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_first())
w_grp = Window.partitionBy("order_id")
w_grp_2 = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id")
w6 = Window.partitionBy("order_id").orderBy(F.col("unit_price_bucket").desc_nulls_last(), "id")
w7 = Window.partitionBy("product_id").orderBy(F.col("cost").desc_nulls_first(), "id")
w8 = Window.partitionBy("order_id").orderBy("quantity", "id")
w9 = Window.partitionBy("product_id", "order_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id")

deduped = (
    df_order_items
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.launch_date"))
)

enriched_order_items = (
    enriched
    .withColumn("quantity_cume_dist", F.cume_dist().over(w2))
    .withColumn("order_id_stddev_samp_unit_price", F.stddev_samp("unit_price").over(w_grp))
    .withColumn("quantity_avg_2", F.avg("quantity").over(w_grp_2))
    .withColumn("unit_price_bucket", F.ntile(5).over(w_ordered))
)

deduped_order_items = (
    enriched_order_items
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

enriched_order_items_2 = (
    deduped_order_items
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.cost"), F.col("p.launch_date").alias("product_launch_date"))
)

deduped_order_items_2 = (
    enriched_order_items_2
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped_order_items_2
    .withColumn("prev_order_id_stddev_samp_unit_price", F.lag("order_id_stddev_samp_unit_price", 1).over(w8))
    .withColumn("running_cost", F.sum("cost").over(w9))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w_running))
)

daily = (
    metrics
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["order_id", "quantity_tier"], ["order_id"]],
        "order_id",
        "quantity_tier",
    )
    .agg(
        F.round(F.avg("order_id_stddev_samp_unit_price"), 2).alias("avg_order_id_stddev_samp_unit_price"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = daily.select("quantity_tier", "num_rows")
