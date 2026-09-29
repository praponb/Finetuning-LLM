from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last())
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("order_id")
w_grp = Window.orderBy(F.col("line_total").desc())
w5 = Window.partitionBy("product_id", "order_id").orderBy(F.col("line_total").desc())
w6 = Window.partitionBy("order_id").orderBy("rn", "id")
w7 = Window.orderBy("line_total", "id")
w8 = Window.orderBy("line_total_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("quantity") != 6)
    .select("*", (F.col("quantity") + F.col("unit_price")).alias("line_total"))
)

ranked = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .filter(F.col("rn") < 1)
    .select(
        "id",
        "quantity",
        "unit_price",
        "line_total",
        "rn",
        F.stddev_samp("quantity").over(w2).alias("product_id_stddev_samp_quantity"),
        F.corr("quantity", "unit_price").over(w3).alias("corr_quantity_unit_price"),
        F.cume_dist().over(w_grp).alias("line_total_cume_dist"),
        F.rank().over(w5).alias("line_total_rank"),
        F.lag("quantity", 2).over(w6).alias("prev_quantity"),
    )
)

filtered_order_items = (
    metrics
    .filter(F.col("quantity").between(3, 5))
    .select(
        "id",
        "quantity",
        "unit_price",
        "line_total",
        "rn",
        "product_id_stddev_samp_quantity",
        "corr_quantity_unit_price",
        "line_total_cume_dist",
        "prev_quantity",
        F.round((F.col("unit_price") / F.col("prev_quantity")), 2).alias("unit_price_per_prev_quantity"),
    )
)

metrics_order_items = (
    filtered_order_items
    .select(
        "*",
        F.lag("unit_price").over(w7).alias("prev_unit_price"),
        F.lag("prev_quantity").over(w7).alias("prev_prev_quantity"),
        F.lead("prev_quantity").over(w7).alias("next_prev_quantity"),
        F.row_number().over(w7).alias("event_seq"),
        F.last("line_total_cume_dist", ignorenulls=True).over(w8).alias("last_known_line_total_cume_dist"),
    )
)

result = (
    metrics_order_items
    .groupBy(
        F.when(F.col("last_known_line_total_cume_dist") >= 0.69, "HIGH").otherwise("LOW").alias("last_known_line_total_cume_dist_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("unit_price_per_prev_quantity"), 2).alias("avg_unit_price_per_prev_quantity"),
    )
    .orderBy(F.col("cnt").desc())
)
