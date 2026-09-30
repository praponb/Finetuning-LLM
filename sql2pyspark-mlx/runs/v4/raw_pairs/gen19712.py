from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("line_total").asc_nulls_first(), "id")
w_grp = Window.partitionBy("order_id")
w_recent = Window.partitionBy("product_id")
w_ordered = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("order_id").isin([1, 4, 16]))
    .withColumn("line_total", (F.col("unit_price") * F.col("quantity")))
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .withColumn("order_id_avg_line_total", F.avg("line_total").over(w_grp))
    .withColumn("product_id_max_line_total", F.max("line_total").over(w_recent))
    .withColumn("cumulative_unit_price", F.sum("unit_price").over(w_ordered))
)

grouped = (
    with_windows
    .withColumn("order_id_avg_line_total_tier", F.when(F.col("order_id_avg_line_total") >= 81, "LARGE").otherwise("SMALL"))
    .withColumn("cumulative_unit_price_tier", F.when(F.col("cumulative_unit_price") >= 1882, "HIGH").otherwise("LOW"))
    .cube("order_id_avg_line_total_tier", "cumulative_unit_price_tier")
    .agg(
        F.percentile_approx("line_total", 0.9).alias("line_total_p90"),
        F.count("*").alias("event_count"),
        F.percentile_approx("cumulative_unit_price", [0.25, 0.5, 0.75]).alias("cumulative_unit_price_quartiles"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("event_count") < 1)
)

result = (
    grouped
    .select(
        "order_id_avg_line_total_tier",
        "cumulative_unit_price_tier",
        "event_count",
        "quantity_stddev",
        "grouping_level",
    )
    .orderBy(F.col("quantity_stddev").asc_nulls_last())
)
