from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("line_total", "id")
w2 = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("order_id").orderBy(F.col("line_total").desc(), "id")
w4 = Window.partitionBy("order_id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([4, 10, 21]))
    .select(
        "id",
        "order_id",
        "quantity",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

with_dims = (
    base
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.status") == "CANCELLED")
    .select("src.*", F.col("o.amount"))
)

enriched = (
    with_dims
    .select(
        "id",
        "order_id",
        "line_total",
        "amount",
        F.ntile(5).over(w).alias("line_total_bucket"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)

with_windows = (
    enriched
    .withColumn("next_amount", F.lead("amount").over(w))
    .withColumn("prev_line_total", F.lag("line_total").over(w))
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("line_total_bucket_2", F.ntile(5).over(w_ordered))
)

matched_keys = (
    with_windows
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id") == 9)
        .select("id", "order_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "*",
        F.round((F.col("line_total_bucket") / F.sum("line_total_bucket").over(w4)), 4).alias("line_total_bucket_share"),
    )
)
