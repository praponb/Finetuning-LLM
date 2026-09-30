from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity_num").asc_nulls_first())
w2 = Window.partitionBy("category", "product_id")
w3 = Window.partitionBy("order_id", "product_id").orderBy("line_total", "id")
w4 = Window.partitionBy("product_id", "order_id").orderBy("quantity_share", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy(F.col("total_prev_quantity_num_percent_rank").desc())

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") >= 5))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.col("quantity").cast("double").alias("quantity_num"),
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.product_name"), F.col("b.category"), F.col("b.stock_qty"))
)

metrics = (
    joined
    .select(
        "id",
        "order_id",
        "product_id",
        "line_total",
        F.percent_rank().over(w).alias("quantity_num_percent_rank"),
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4).alias("quantity_share"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity_num_percent_rank",
        F.lag("quantity_num_percent_rank").over(w3).alias("prev_quantity_num_percent_rank"),
        F.lead("quantity_share").over(w3).alias("next_quantity_share"),
        F.lag("quantity_share").over(w3).alias("prev_quantity_share"),
        F.row_number().over(w3).alias("event_seq"),
        F.avg("quantity_share").over(w4).alias("quantity_share_avg_6"),
    )
)

result = (
    with_windows
    .groupBy(
        "product_id",
        F.when(F.col("next_quantity_share") >= 0.8, "PREMIUM").otherwise("STANDARD").alias("next_quantity_share_tier"),
        F.when(F.col("prev_quantity_num_percent_rank") >= 0.87, "HIGH").otherwise("LOW").alias("prev_quantity_num_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("prev_quantity_share", 0.5).alias("prev_quantity_share_p50"),
        F.round(F.avg("next_quantity_share"), 2).alias("avg_next_quantity_share"),
        F.round(F.avg("quantity_share_avg_6"), 2).alias("avg_quantity_share_avg_6"),
        F.sum("prev_quantity_num_percent_rank").alias("total_prev_quantity_num_percent_rank"),
        F.avg("quantity_num_percent_rank").alias("avg_quantity_num_percent_rank"),
    )
    .withColumn("total_prev_quantity_num_percent_rank_dense_rank", F.dense_rank().over(w5))
    .orderBy(F.col("avg_quantity_num_percent_rank").desc_nulls_last())
)
