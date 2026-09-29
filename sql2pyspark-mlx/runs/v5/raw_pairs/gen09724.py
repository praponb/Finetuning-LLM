from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("line_total", "id")
w_ordered = Window.orderBy("running_count_line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("unit_price").desc())
w_recent = Window.orderBy("unit_price", "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

scored = (
    filtered
    .filter(F.col("unit_price").between(200, 390))
    .withColumn("running_count_line_total", F.count("line_total").over(w))
    .withColumn("running_sum_line_total", F.sum("line_total").over(w))
    .withColumn("running_avg_line_total", F.avg("line_total").over(w))
    .withColumn("prev_unit_price", F.lag("unit_price", 1, 0).over(w2))
)

base = (
    scored
    .filter(F.col("unit_price") < 110)
    .select(
        "id",
        "quantity",
        "unit_price",
        "line_total",
        "running_count_line_total",
        "running_sum_line_total",
        F.greatest("running_count_line_total", "quantity").alias("greatest_running_count_line_total_quantity"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.count("line_total").over(w_ordered).alias("running_count_line_total_2"),
        F.max("line_total").over(w_ordered).alias("running_max_line_total"),
        F.sum("line_total").over(w_ordered).alias("running_sum_line_total_2"),
        F.min("line_total").over(w_ordered).alias("running_min_line_total"),
    )
)

metrics_order_items = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w4).alias("unit_price_cume_dist"),
        F.lag("running_count_line_total", 1, 0).over(w_recent).alias("prev_running_count_line_total"),
    )
)

result = (
    metrics_order_items
    .select(
        "id",
        "unit_price",
        "line_total",
        "running_count_line_total",
        "running_sum_line_total",
        "greatest_running_count_line_total_quantity",
        "running_count_line_total_2",
        "running_sum_line_total_2",
        "running_min_line_total",
    )
    .orderBy(F.col("running_sum_line_total_2").desc())
)
