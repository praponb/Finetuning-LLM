from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy("unit_price_times_unit_price_avg_3", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("order_id").orderBy("unit_price_times_unit_price_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-2, -1)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)

metrics = (
    valid
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w_hist))
    .withColumn("unit_price_avg_3", F.avg("unit_price").over(w2))
    .withColumn("order_id_stddev_samp_line_total", F.stddev_samp("line_total").over(w3))
)

valid_order_items = (
    metrics
    .filter(F.col("quantity") <= 6)
    .select(
        "*",
        F.when(F.col("unit_price_avg_3") >= 389, "High").when(F.col("unit_price_avg_3") >= 81, "Medium").otherwise("Low").alias("unit_price_avg_3_tier"),
        (F.col("unit_price") * F.col("unit_price_avg_3")).alias("unit_price_times_unit_price_avg_3"),
    )
)

with_windows = (
    valid_order_items
    .select(
        "*",
        F.avg("unit_price").over(w4).alias("unit_price_avg_3_prior"),
        F.sum("order_id_stddev_samp_line_total").over(w5).alias("cumulative_order_id_stddev_samp_line_total"),
        F.avg("quantity").over(w6).alias("quantity_avg_2_prior"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("line_total") >= 79, "PREMIUM").otherwise("STANDARD").alias("line_total_tier"),
        "order_id",
    )
    .agg(
        F.round(F.avg("quantity_avg_2_prior"), 2).alias("avg_quantity_avg_2_prior"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
    )
    .orderBy(F.col("order_id").asc_nulls_first())
)
