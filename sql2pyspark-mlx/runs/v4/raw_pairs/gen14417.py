from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "product_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())
w5 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(-5, -1)
w_hist = Window.orderBy("cumulative_line_total_2", "id").rowsBetween(-5, -1)
w7 = Window.orderBy(F.col("line_total_avg_5_prior").desc(), "id")
w8 = Window.partitionBy("order_id", "product_id")
w9 = Window.partitionBy("product_id").orderBy(F.col("cumulative_line_total").desc_nulls_last())
w10 = Window.partitionBy("product_id").orderBy("cumulative_line_total", "id")

base = (
    df_order_items
    .filter(F.col("quantity").between(4, 5))
    .withColumn("unit_price_level", F.when(F.col("unit_price") >= 350, "High").when(F.col("unit_price") >= 150, "Medium").otherwise("Low"))
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

scored = (
    base
    .withColumn("cumulative_line_total", F.sum("line_total").over(w))
    .withColumn("cumulative_unit_price", F.sum("unit_price").over(w2))
    .withColumn("cumulative_line_total_2", F.sum("line_total").over(w3))
    .withColumn("unit_price_percent_rank", F.percent_rank().over(w4))
    .withColumn("line_total_avg_5_prior", F.avg("line_total").over(w5))
)

with_windows = (
    scored
    .select(
        "*",
        F.avg("cumulative_line_total_2").over(w_hist).alias("cumulative_line_total_2_avg_5_prior"),
        F.ntile(10).over(w7).alias("line_total_avg_5_prior_bucket"),
        F.count("line_total").over(w8).alias("order_id_count_line_total"),
        F.cume_dist().over(w9).alias("cumulative_line_total_cume_dist"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.amount"), F.col("d.status"), F.col("d.order_date"))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price_level",
        "line_total",
        "cumulative_unit_price",
        "unit_price_percent_rank",
        "line_total_avg_5_prior",
    )
    .orderBy(F.col("product_id").desc())
)
