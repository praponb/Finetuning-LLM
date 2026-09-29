from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy(F.col("line_total").asc_nulls_first())
w_recent = Window.partitionBy("product_id").orderBy("unit_price", "id")
w4 = Window.orderBy(F.col("line_total").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("line_total").desc(), "id")
w6 = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("product_id")
w8 = Window.partitionBy("product_id").orderBy("product_id_min_line_total_percent_rank", "id")
w9 = Window.orderBy("unit_price", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") <= 4))
    .select("*", (F.col("quantity") + F.col("unit_price")).alias("line_total"))
)

enriched = (
    valid
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.supplier_id") == 1)
    .select("src.*", F.col("d.product_name"), F.col("d.cost"))
)

scored = (
    enriched
    .select(
        "id",
        "product_id",
        "unit_price",
        "line_total",
        F.corr("quantity", "unit_price").over(w).alias("corr_quantity_unit_price"),
        F.percent_rank().over(w2).alias("line_total_percent_rank"),
    )
)

scored_order_items = (
    scored
    .withColumn("next_line_total_percent_rank", F.lead("line_total_percent_rank").over(w_recent))
    .withColumn("prev_line_total_percent_rank", F.lag("line_total_percent_rank").over(w_recent))
    .withColumn("prev_line_total", F.lag("line_total").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("line_total_bucket", F.ntile(5).over(w4))
)

deduped = (
    scored_order_items
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.sum("corr_quantity_unit_price").over(w6).alias("running_corr_quantity_unit_price"),
        F.min("line_total_percent_rank").over(w_hist).alias("product_id_min_line_total_percent_rank"),
        F.sum("prev_line_total").over(w_hist).alias("product_id_sum_prev_line_total"),
    )
)

result = (
    metrics
    .select(
        "id",
        "product_id",
        "unit_price",
        "line_total_percent_rank",
        "next_line_total_percent_rank",
        "prev_line_total_percent_rank",
        "prev_line_total",
        "row_seq",
        "product_id_min_line_total_percent_rank",
        F.row_number().over(w8).alias("row_seq_2"),
        F.stddev_samp("unit_price").over(w_hist).alias("product_id_stddev_samp_unit_price"),
        F.ntile(10).over(w9).alias("unit_price_bucket"),
    )
)
