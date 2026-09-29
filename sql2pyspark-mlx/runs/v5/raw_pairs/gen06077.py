from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.partitionBy("category")
w4 = Window.orderBy("quantity", "id")
w5 = Window.orderBy("quantity", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w7 = Window.orderBy("stock_qty_avg_5_prior_per_quantity")
w_ordered = Window.orderBy("stock_qty_avg_5_prior_per_quantity_percent_rank", "id")

latest_match = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.category", "s.stock_qty", F.col("c.quantity"))
)

scored = (
    latest_match
    .filter(F.col("stock_qty") >= 263)
    .select(
        "*",
        F.ntile(3).over(w2).alias("stock_qty_bucket"),
        F.count("stock_qty").over(w3).alias("category_count_stock_qty"),
        F.lead("quantity", 2, 0).over(w4).alias("next_quantity"),
        F.avg("stock_qty").over(w5).alias("stock_qty_avg_5_prior"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.stock_qty",
        "s.quantity",
        "s.stock_qty_bucket",
        "s.stock_qty_avg_5_prior",
        F.col("c.unit_price"),
    )
)

cleaned = (
    with_last
    .filter(F.col("stock_qty_avg_5_prior") == 172)
    .select(
        "id",
        "quantity",
        "stock_qty_bucket",
        "stock_qty_avg_5_prior",
        "unit_price",
        (F.col("stock_qty_avg_5_prior") / F.nullif("quantity", F.lit(0))).alias("stock_qty_avg_5_prior_per_quantity"),
    )
)

enriched = (
    cleaned
    .withColumn("stock_qty_avg_5_prior_per_quantity_percent_rank", F.percent_rank().over(w7))
    .withColumn("seq_no", F.row_number().over(w4))
)

kept = (
    enriched
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock").between("2023-03-01", "2024-06-05")), "left_anti")
)

with_windows = (
    kept
    .select(
        "id",
        "stock_qty_bucket",
        "seq_no",
        F.lag("seq_no", 1).over(w_ordered).alias("prev_seq_no"),
        F.row_number().over(w_ordered).alias("seq_no_2"),
    )
)

result = (
    with_windows
    .groupBy("seq_no")
    .agg(
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.percentile_approx("seq_no_2", [0.25, 0.5, 0.75]).alias("seq_no_2_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("prev_seq_no").alias("avg_prev_seq_no"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("seq_no").asc_nulls_last())
)
