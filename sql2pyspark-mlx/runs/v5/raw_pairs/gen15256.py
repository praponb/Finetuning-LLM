from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("line_total", "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("line_total_bucket", "id")
w_grp = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id", "order_id").orderBy("first_known_quantity_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id")
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

filtered = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
    .withColumn("quantity_num", F.col("quantity").cast("double"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.ntile(3).over(w).alias("line_total_bucket"),
        F.first("quantity_num", ignorenulls=True).over(w2).alias("first_known_quantity_num"),
    )
)

valid = (
    with_windows
    .filter(F.col("product_id").isNotNull() & F.col("first_known_quantity_num").between(34, 51))
    .withColumn("line_total_bucket_times_quantity_num", (F.col("line_total_bucket") * F.col("quantity_num")))
)

scored = (
    valid
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "line_total",
        "first_known_quantity_num",
        F.lag("quantity", 2).over(w3).alias("prev_quantity"),
        F.first("quantity_num", ignorenulls=True).over(w_grp).alias("first_known_quantity_num_2"),
        F.sum("unit_price").over(w_grp).alias("cumulative_unit_price"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "order_id",
        "prev_quantity",
        F.avg("prev_quantity").over(w5).alias("running_avg_prev_quantity"),
        F.sum("prev_quantity").over(w5).alias("running_sum_prev_quantity"),
        F.min("prev_quantity").over(w5).alias("running_min_prev_quantity"),
        F.max("prev_quantity").over(w5).alias("running_max_prev_quantity"),
        F.sum("cumulative_unit_price").over(w6).alias("product_id_sum_cumulative_unit_price"),
    )
)

grouped = (
    metrics
    .withColumn("running_max_prev_quantity_tier", F.when(F.col("running_max_prev_quantity") >= 9.0, "PREMIUM").otherwise("STANDARD"))
    .cube("order_id", "running_max_prev_quantity_tier")
    .agg(
        F.percentile_approx("running_min_prev_quantity", [0.25, 0.5, 0.75]).alias("running_min_prev_quantity_quartiles"),
        F.count("*").alias("row_count"),
        F.grouping("order_id").alias("grouping_id"),
    )
    .filter(F.col("row_count") < 1000)
)

result = (
    grouped
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "running_max_prev_quantity_tier",
        "running_min_prev_quantity_quartiles",
        "grouping_id",
        F.col("min_order_item_unit_price"),
    )
    .orderBy("grouping_id")
)
