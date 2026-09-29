from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity_filled").desc_nulls_first())
w3 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "order_id").orderBy("row_seq", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("order_id", "product_id").orderBy("row_seq", "id")
w6 = Window.orderBy(F.col("row_seq").desc(), "id")
w7 = Window.orderBy("quantity_filled", "id")
w8 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity_filled").desc())
w9 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("first_known_quantity_filled_percent_rank", "id")
w11 = Window.partitionBy("order_id")
w12 = Window.partitionBy("product_id", "order_id").orderBy("row_seq_bucket_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("order_id").orderBy("first_known_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w15 = Window.partitionBy("order_id").orderBy("prev_row_seq_bucket_whole", "id").rowsBetween(-5, -1)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

scored = (
    valid
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("quantity_filled_percent_rank", F.percent_rank().over(w2))
)

with_windows = (
    scored
    .withColumn("first_known_quantity_filled_percent_rank", F.first("quantity_filled_percent_rank", ignorenulls=True).over(w3))
    .withColumn("row_seq_avg_6_prior", F.avg("row_seq").over(w4))
    .withColumn("row_seq_bucket", F.ntile(4).over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

cleaned = (
    with_windows
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") > 390))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_filled",
        "first_known_quantity_filled_percent_rank",
        "row_seq_avg_6_prior",
        F.floor("row_seq_bucket").alias("row_seq_bucket_whole"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lag("row_seq_bucket_whole", 1).over(w7).alias("prev_row_seq_bucket_whole"),
        F.cume_dist().over(w8).alias("quantity_filled_cume_dist"),
        F.first("unit_price", ignorenulls=True).over(w9).alias("first_known_unit_price"),
        F.lead("row_seq_bucket_whole", 1).over(w10).alias("next_row_seq_bucket_whole"),
        F.avg("row_seq_bucket_whole").over(w11).alias("order_id_avg_row_seq_bucket_whole"),
    )
)

enriched_order_items = (
    enriched
    .select(
        "*",
        F.last("prev_row_seq_bucket_whole", ignorenulls=True).over(w12).alias("last_known_prev_row_seq_bucket_whole"),
        F.sum("quantity_filled_cume_dist").over(w13).alias("running_quantity_filled_cume_dist"),
        F.sum("first_known_unit_price").over(w14).alias("running_first_known_unit_price"),
        F.avg("order_id_avg_row_seq_bucket_whole").over(w15).alias("order_id_avg_row_seq_bucket_whole_avg_5_prior"),
    )
)

joined = (
    enriched_order_items
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.category"), F.col("p.price"))
)

daily = (
    joined
    .groupBy(
        F.when(F.col("quantity_filled_cume_dist") >= 0.62, "LARGE").otherwise("SMALL").alias("quantity_filled_cume_dist_tier"),
        "category",
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("running_first_known_unit_price", 0.5).alias("running_first_known_unit_price_p50"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.sort_array(F.collect_set("category")).alias("category_list_2"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
)

result = (
    daily
    .select(
        "quantity_filled_cume_dist_tier",
        "category",
        "running_first_known_unit_price_p50",
        "category_list",
        "category_list_2",
        "num_categories",
    )
    .orderBy("num_categories")
)
