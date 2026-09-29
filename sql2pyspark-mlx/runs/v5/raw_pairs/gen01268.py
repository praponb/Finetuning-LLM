from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy("order_date", "id")

cleaned = (
    df_order_items
    .filter(F.col("order_id") == 8)
    .select(
        "*",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "quantity", "unit_price", "greatest_unit_price_quantity")
)

with_windows = (
    deduped
    .filter(F.col("greatest_unit_price_quantity") > 26)
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.corr("quantity", "unit_price").over(w3).alias("corr_quantity_unit_price"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.order_date"), F.col("d.amount"))
)

with_windows_order_items = (
    with_dims
    .select(
        "*",
        F.row_number().over(w4).alias("seq_no_2"),
        F.corr("quantity", "unit_price").over(w3).alias("corr_quantity_unit_price_2"),
    )
)

grouped = (
    with_windows_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
        F.percentile_approx("seq_no_2", [0.25, 0.5, 0.75]).alias("seq_no_2_quartiles"),
    )
    .filter(F.col("seq_no_p95") > 0)
)

result = (
    grouped
    .select("quantity_tier", "seq_no_p95", "seq_no_2_quartiles")
    .orderBy(F.col("quantity_tier").desc_nulls_last())
)
