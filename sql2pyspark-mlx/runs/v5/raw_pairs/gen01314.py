from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w_part = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
        F.when(F.col("unit_price") >= 310, "High").when(F.col("unit_price") >= 70, "Medium").otherwise("Low").alias("unit_price_level"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "product_id", "unit_price", "line_total")
)

metrics = (
    ranked
    .select(
        "*",
        F.avg("line_total").over(w_part).alias("last7_avg_line_total"),
        F.max("line_total").over(w_part).alias("last7_max_line_total"),
        F.min("line_total").over(w_part).alias("last7_min_line_total"),
        F.sum("line_total").over(w_part).alias("last7_sum_line_total"),
    )
)

base = (
    metrics
    .filter(F.col("last7_max_line_total").between(30, 61))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "last7_avg_line_total",
        "last7_max_line_total",
        "last7_sum_line_total",
        (F.col("line_total") * F.col("last7_avg_line_total")).alias("line_total_times_last7_avg_line_total"),
    )
)

result = (
    base
    .select(
        "id",
        "unit_price",
        "last7_avg_line_total",
        "line_total_times_last7_avg_line_total",
    )
    .orderBy(F.col("line_total_times_last7_avg_line_total").desc(), "unit_price")
)
