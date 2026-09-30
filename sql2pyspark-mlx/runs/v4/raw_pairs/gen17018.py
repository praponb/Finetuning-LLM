from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").asc_nulls_last())
w3 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w5 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_hist = Window.partitionBy("product_id", "order_id")
w7 = Window.partitionBy("order_id").orderBy("unit_price_cume_dist", "id")
w8 = Window.partitionBy("order_id", "product_id").orderBy(F.col("prev_seq_no").desc(), "id")
w9 = Window.orderBy("unit_price_avg_3", "id")
w10 = Window.partitionBy("order_id")
w11 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

scored = (
    df_order_items
    .withColumn("running_unit_price", F.sum("unit_price").over(w))
    .withColumn("unit_price_cume_dist", F.cume_dist().over(w2))
    .withColumn("unit_price_avg_3", F.avg("unit_price").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

with_windows = (
    scored
    .select(
        "*",
        F.row_number().over(w5).alias("row_seq"),
        F.sum("running_unit_price").over(w_hist).alias("product_id_sum_running_unit_price"),
        F.lag("seq_no", 2).over(w7).alias("prev_seq_no"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    ranked
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.cost"))
)

scored_order_items = (
    enriched
    .select(
        "*",
        F.lead("row_seq", 1).over(w9).alias("next_row_seq"),
        F.round((F.col("cost") / F.sum("cost").over(w10)), 4).alias("cost_share"),
    )
)

metrics = (
    scored_order_items
    .select(
        "*",
        F.percent_rank().over(w11).alias("quantity_percent_rank"),
        F.avg("unit_price_avg_3").over(w10).alias("order_id_avg_unit_price_avg_3"),
        F.lag("seq_no", 2).over(w5).alias("prev_seq_no_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "order_id",
        "unit_price_cume_dist",
        "unit_price",
        "quantity_percent_rank",
    )
    .unpivot(
        ["id", "order_id"],
        ["unit_price_cume_dist", "unit_price", "quantity_percent_rank"],
        "metric_name",
        "value",
    )
)
