from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc(), "id", "item_no")
w2 = Window.orderBy("price", "id", "item_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("product_id", "id", "item_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("running_price", "id", "item_no")
w_grp = Window.orderBy("cumulative_product_id_minus_item_no", "id", "item_no")
w_part = Window.orderBy("running_price", "id", "item_no").rowsBetween(-5, Window.currentRow)
w7 = Window.orderBy("item_no_avg_5", "id", "item_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("cumulative_product_id_minus_item_no_bucket").desc(), "id", "item_no")
w9 = Window.orderBy(F.col("price").asc_nulls_last())
w10 = Window.orderBy("item_no", "id", "item_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("cumulative_item_no", "id", "item_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("cumulative_product_id_minus_item_no_bucket", "id", "item_no").rowsBetween(-3, -1)
w13 = Window.orderBy("product_id_minus_item_no", "id", "item_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "rec"))
    .select(
        "id",
        (F.col("pos") + 1).alias("item_no"),
        F.col("rec.product_id"),
        F.col("rec.price"),
    )
)

base = (
    exploded
    .filter(F.col("price") <= 100)
    .select(
        "*",
        (F.col("product_id") - F.col("item_no")).alias("product_id_minus_item_no"),
    )
)

scored = (
    base
    .filter(F.col("product_id") != 3)
    .select(
        "*",
        F.ntile(10).over(w).alias("price_bucket"),
        F.sum("product_id_minus_item_no").over(w2).alias("cumulative_product_id_minus_item_no"),
        F.sum("item_no").over(w3).alias("cumulative_item_no"),
        F.sum("price").over(w3).alias("running_price"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.lead("running_price", 2).over(w_recent).alias("next_running_price"),
        F.ntile(3).over(w_grp).alias("cumulative_product_id_minus_item_no_bucket"),
        F.avg("item_no").over(w_part).alias("item_no_avg_5"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.first("cumulative_product_id_minus_item_no_bucket", ignorenulls=True).over(w7).alias("first_known_cumulative_product_id_minus_item_no_bucket"),
        F.ntile(10).over(w8).alias("cumulative_product_id_minus_item_no_bucket_bucket"),
    )
)

metrics_carts = (
    metrics
    .withColumn("price_dense_rank", F.dense_rank().over(w9))
    .withColumn("last_known_item_no", F.last("item_no", ignorenulls=True).over(w10))
    .withColumn("last_known_next_running_price", F.last("next_running_price", ignorenulls=True).over(w11))
    .withColumn("price_avg_3_prior", F.avg("price").over(w12))
    .withColumn("first_known_first_known_cumulative_product_id_minus_item_no_bucket", F.first("first_known_cumulative_product_id_minus_item_no_bucket", ignorenulls=True).over(w13))
)

result = (
    metrics_carts
    .groupBy("product_id")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("cumulative_item_no", [0.25, 0.5, 0.75]).alias("cumulative_item_no_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
