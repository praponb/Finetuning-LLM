from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("line_total").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w_hist = Window.partitionBy("order_id").orderBy("line_total", "id")
w4 = Window.orderBy(F.col("quantity").desc(), "id")
w_ordered = Window.partitionBy("order_id")
w6 = Window.partitionBy("order_id").orderBy(F.col("order_id_avg_quantity").asc_nulls_last(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)

with_windows = (
    filtered
    .withColumn("line_total_bucket", F.ntile(5).over(w))
    .withColumn("next_line_total", F.lead("line_total", 2).over(w2))
    .withColumn("line_total_bucket_2", F.ntile(10).over(w_hist))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("order_id_avg_quantity", F.avg("quantity").over(w_ordered))
)

cleaned = (
    with_windows
    .filter(F.col("quantity").between(5, 8))
    .withColumn("seq_no_times_order_id_avg_quantity", (F.col("seq_no") * F.col("order_id_avg_quantity")))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

stats = (
    ranked
    .groupBy(
        F.when(F.col("seq_no") >= 9, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
        "product_id",
    )
    .agg(
        F.min("next_line_total").alias("min_next_line_total"),
        F.percentile_approx("line_total_bucket_2", [0.25, 0.5, 0.75]).alias("line_total_bucket_2_quartiles"),
    )
    .filter(F.col("min_next_line_total") < 0)
)

result = (
    stats
    .select(
        "seq_no_tier",
        "product_id",
        "min_next_line_total",
        "line_total_bucket_2_quartiles",
    )
    .orderBy(F.col("min_next_line_total").desc_nulls_first())
)
