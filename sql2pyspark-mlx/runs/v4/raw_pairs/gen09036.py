from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("line_total", "id")
w2 = Window.orderBy("quantity_scaled", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_scaled"),
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

valid = (
    filtered
    .filter(F.col("line_total") < 23)
    .select(
        "*",
        (F.col("line_total") * F.col("quantity")).alias("line_total_times_quantity"),
        (F.col("quantity_scaled") + F.col("quantity")).alias("quantity_scaled_plus_quantity"),
    )
)

cleaned = (
    valid
    .filter(F.col("quantity_scaled") < 66)
    .select(
        "id",
        "quantity",
        "unit_price",
        "quantity_scaled",
        "line_total",
        "quantity_scaled_plus_quantity",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

cleaned_order_items = (
    cleaned
    .filter(F.col("quantity") != 5)
    .withColumn("line_total_filled", F.coalesce("line_total", F.lit(0)))
)

metrics = (
    cleaned_order_items
    .select(
        "*",
        F.ntile(10).over(w_running).alias("line_total_bucket"),
        F.avg("line_total_filled").over(w2).alias("line_total_filled_avg_3"),
        F.ntile(3).over(w_running).alias("line_total_bucket_2"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("quantity_filled") >= 69, "PREMIUM").otherwise("STANDARD").alias("quantity_filled_tier"),
    )
    .agg(
        F.percentile_approx("line_total", [0.25, 0.5, 0.75]).alias("line_total_quartiles"),
        F.round(F.avg("quantity_scaled_plus_quantity"), 2).alias("avg_quantity_scaled_plus_quantity"),
    )
)
