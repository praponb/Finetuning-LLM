from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("method", "order_id").orderBy("method_length", "id")
w3 = Window.partitionBy("method").orderBy("amount_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("method_length", "id")
w5 = Window.partitionBy("order_id", "method").orderBy("amount_rounded", "id")
w6 = Window.partitionBy("status").orderBy(F.col("next_amount").asc_nulls_last(), "id")
w7 = Window.partitionBy("order_id").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy(F.col("method_length").desc())
w9 = Window.partitionBy("status").orderBy("method_length", "id")
w_recent = Window.partitionBy("status")
w11 = Window.partitionBy("method").orderBy("method_length", "id")
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-08-17").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.length("method").alias("method_length"),
        F.round("amount", 0).alias("amount_rounded"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

with_windows = (
    cleaned
    .withColumn("cumulative_amount", F.sum("amount").over(w))
    .withColumn("next_amount_rounded", F.lead("amount_rounded", 1).over(w_part))
    .withColumn("last_known_amount_rounded", F.last("amount_rounded", ignorenulls=True).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("next_amount", F.lead("amount", 1).over(w5))
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

with_windows_payments = (
    ranked
    .select(
        "*",
        F.first("next_amount_rounded", ignorenulls=True).over(w7).alias("first_known_next_amount_rounded"),
        F.dense_rank().over(w8).alias("method_length_dense_rank"),
        F.row_number().over(w9).alias("event_seq"),
        F.min("amount").over(w_recent).alias("status_min_amount"),
        F.row_number().over(w11).alias("seq_no"),
    )
)

joined = (
    with_windows_payments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.amount").alias("order_amount"),
        F.col("p.shipping_fee"),
        F.col("p.order_date"),
    )
)

result = (
    joined
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "method_length",
        "amount_rounded",
        "method_clean",
        "cumulative_amount",
        "next_amount_rounded",
        "row_num",
        F.col("sum_order_item_unit_price"),
    )
    .orderBy(F.col("method").asc_nulls_first(), "method_length")
)
