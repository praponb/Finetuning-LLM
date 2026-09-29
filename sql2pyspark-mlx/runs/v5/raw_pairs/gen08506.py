from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy(F.col("amount").desc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w5 = Window.orderBy("order_item_quantity", "id")
w6 = Window.orderBy("order_date", "id")

ranked = (
    df_orders
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_match = (
    latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.order_date",
        "s.status",
        "s.amount",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

cleaned = (
    latest_match
    .filter(F.col("order_date") >= F.lit("2023-07-26").cast("date"))
    .select(
        "id",
        "order_date",
        "order_item_quantity",
        F.upper("status").alias("status_upper"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.paid_at"), F.col("c.amount"))
)

result = (
    with_latest
    .select(
        "id",
        "paid_at",
        "amount",
        F.lead("amount").over(w5).alias("next_amount"),
        F.lag("amount").over(w5).alias("prev_amount"),
        F.lead("order_item_quantity").over(w5).alias("next_order_item_quantity"),
        F.lag("order_item_quantity").over(w5).alias("prev_order_item_quantity"),
        F.row_number().over(w5).alias("event_seq"),
        F.lag("order_item_quantity", 1).over(w6).alias("prev_order_item_quantity_2"),
    )
)
