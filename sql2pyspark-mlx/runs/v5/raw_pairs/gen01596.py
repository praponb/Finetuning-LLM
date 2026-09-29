from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("amount").desc())
w4 = Window.partitionBy("status", "channel").orderBy("shipping_fee", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("amount", "id")

valid = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-03-16").cast("date"))
    .withColumn("order_date_label", F.date_format("order_date", "MMM yyyy"))
)

with_windows = (
    valid
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
        F.last("channel", ignorenulls=True).over(w2).alias("last_known_channel"),
        F.rank().over(w3).alias("amount_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "discount",
        "shipping_fee",
        "first_known_amount",
        "last_known_channel",
        F.avg("amount").over(w4).alias("amount_avg_2"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .intersect(
        df_orders
        .filter(F.col("shipping_fee") < 13)
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid_orders = (
    verified
    .filter(F.col("discount") <= 0.12)
    .select(
        "id",
        "first_known_amount",
        "last_known_channel",
        "row_seq",
        F.round((F.col("discount") * 0.9), 2).alias("discount_adjusted"),
        (F.col("first_known_amount") - F.col("shipping_fee")).alias("first_known_amount_minus_shipping_fee"),
    )
)

result = (
    valid_orders
    .select("id", "last_known_channel", "row_seq", "discount_adjusted")
    .orderBy(F.col("id").desc())
)
