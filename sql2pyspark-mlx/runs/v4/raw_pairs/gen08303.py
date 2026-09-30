from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("brand").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id")

cleaned = (
    df_order_items
    .filter(F.col("quantity") > 3)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("quantity").cast("double").alias("quantity_num"),
    )
)

filtered = (
    cleaned
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([8, 30]))
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
    .withColumn("quantity_times_quantity_num", (F.col("quantity") * F.col("quantity_num")))
)

enriched = (
    filtered
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.brand"), F.col("b.product_name"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.row_number().over(w_running).alias("row_seq"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.corr("unit_price", "quantity").over(w_recent).alias("corr_unit_price_quantity"),
        F.min("quantity").over(w_recent).alias("product_id_min_quantity"),
    )
)

grouped = (
    with_windows
    .groupBy("brand")
    .agg(
        F.round(F.sum("line_total") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("line_total_per_stark"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.count_if(F.col("brand") == "Stark").alias("stark_events"),
        F.max(F.when(F.col("brand") == "Umbrella", F.col("quantity_num"))).alias("umbrella_max_quantity_num"),
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
    )
    .filter(F.col("stark_events") >= 1)
)

result = (
    grouped
    .select(
        "brand",
        "line_total_per_stark",
        "num_brands",
        "stark_events",
        "umbrella_max_quantity_num",
        "row_seq_p95",
    )
    .orderBy(F.col("umbrella_max_quantity_num").desc(), F.col("brand").desc())
    .limit(20)
)
