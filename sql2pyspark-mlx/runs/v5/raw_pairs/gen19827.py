from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("line_total").asc_nulls_first(), "id")
w3 = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "order_id").orderBy("unit_price_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy("product_id").orderBy(F.col("rn").desc(), "id")

valid = (
    df_order_items
    .filter(F.col("quantity") > 3)
    .select("*", (F.col("quantity") - F.col("unit_price")).alias("line_total"))
)

valid_order_items = (
    valid
    .filter(F.col("product_id").isin([9, 28, 30, 34]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "line_total",
        F.floor("unit_price").alias("unit_price_whole"),
    )
)

latest = (
    valid_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "order_id",
        "product_id",
        "line_total",
        "rn",
        F.sum("unit_price_whole").over(w3).alias("running_unit_price_whole"),
        F.sum("line_total").over(w4).alias("cumulative_line_total"),
        F.avg("line_total").over(w_hist).alias("line_total_avg_2"),
        F.min("line_total").over(w6).alias("product_id_min_line_total"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy(
        "order_id",
        "product_id",
        F.when(F.col("product_id_min_line_total") >= 90, "PREMIUM").otherwise("STANDARD").alias("product_id_min_line_total_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_line_total", [0.25, 0.5, 0.75]).alias("cumulative_line_total_quartiles"),
        F.percentile_approx("running_unit_price_whole", 0.5).alias("running_unit_price_whole_p50"),
        F.percentile_approx("product_id_min_line_total", [0.25, 0.5, 0.75]).alias("product_id_min_line_total_quartiles"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("line_total_avg_2", 0.5).alias("line_total_avg_2_p50"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)
