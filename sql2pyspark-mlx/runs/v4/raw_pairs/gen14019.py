from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.orderBy(F.col("line_total").desc(), "id")
w3 = Window.orderBy(F.col("seq_no").asc_nulls_last())
w4 = Window.orderBy(F.col("line_total").desc_nulls_first())
w5 = Window.orderBy("seq_no_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("quantity").desc_nulls_last())
w7 = Window.orderBy(F.col("unit_price").desc())

base = (
    df_order_items
    .filter(F.col("quantity") != 4)
    .select(
        "id",
        "quantity",
        "unit_price",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

with_windows = (
    base
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("line_total_bucket", F.ntile(10).over(w2))
)

metrics = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w3).alias("seq_no_dense_rank"),
        F.cume_dist().over(w4).alias("line_total_cume_dist"),
    )
)

metrics_order_items = (
    metrics
    .withColumn("first_known_unit_price", F.first("unit_price", ignorenulls=True).over(w5))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w6))
    .withColumn("unit_price_rank", F.rank().over(w7))
)

valid = (
    metrics_order_items
    .filter(F.col("unit_price_rank").between(4, 5))
    .select(
        "id",
        "quantity",
        "unit_price",
        "line_total",
        "line_total_bucket",
        "seq_no_dense_rank",
        "line_total_cume_dist",
        "quantity_dense_rank",
        "unit_price_rank",
        (F.col("quantity_dense_rank") + F.col("quantity")).alias("quantity_dense_rank_plus_quantity"),
    )
)

daily = (
    valid
    .groupBy(
        F.when(F.col("seq_no_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("seq_no_dense_rank_tier"),
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("cnt"),
    )
)

result = (
    daily
    .select("seq_no_dense_rank_tier", "quantity_bit_and")
    .orderBy(F.col("seq_no_dense_rank_tier").desc_nulls_last())
)
