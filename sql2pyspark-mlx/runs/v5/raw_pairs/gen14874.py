from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("store_id", "channel").orderBy("discount", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w_ordered = Window.orderBy("discount", "id")
w_recent = Window.orderBy("amount", "id")
w6 = Window.orderBy(F.col("discount").desc())
w7 = Window.orderBy("order_date", "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

scored = (
    df_orders
    .filter(F.col("order_date") <= "2025-02-03")
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.lead("discount", 2).over(w2).alias("next_discount"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.order_date",
        "s.amount",
        "s.discount",
        "s.amount_share",
        F.col("c.quantity"),
    )
)

scored_orders = (
    latest_match
    .select(
        "*",
        F.lead("discount", 2).over(w_ordered).alias("next_discount"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.rank().over(w6).alias("discount_rank"),
        F.row_number().over(w7).alias("seq_no"),
        F.row_number().over(w_recent).alias("seq_no_2"),
    )
)

filtered = (
    scored_orders
    .filter(F.col("order_date") >= F.lit("2023-06-14").cast("date"))
    .select(
        "id",
        "amount",
        "quantity",
        "next_discount",
        "event_seq",
        "seq_no",
        "seq_no_2",
        F.least("amount_share", "seq_no").alias("least_amount_share_seq_no"),
        F.round((F.col("amount_share") / F.col("seq_no_2")), 2).alias("amount_share_per_seq_no_2"),
    )
)

cleaned = (
    filtered
    .filter(F.col("least_amount_share_seq_no").between(45, 65))
    .select(
        "id",
        "amount",
        "quantity",
        "next_discount",
        "seq_no",
        "least_amount_share_seq_no",
        "amount_share_per_seq_no_2",
        (F.col("amount_share_per_seq_no_2") * F.col("seq_no")).alias("amount_share_per_seq_no_2_times_seq_no"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.next_discount",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

with_last_orders = (
    with_last
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.next_discount",
        "s.order_item_quantity",
        F.col("c.amount").alias("payment_amount"),
        F.col("c.status").alias("payment_status"),
    )
)

grouped = (
    with_last_orders
    .cube(
        F.when(F.col("order_item_quantity") >= 7, "HIGH").otherwise("LOW").alias("order_item_quantity_tier"),
        F.when(F.col("next_discount") >= 0.36, "HIGH").otherwise("LOW").alias("next_discount_tier"),
    )
    .agg(
        F.countDistinct("payment_status").alias("distinct_payment_status"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") < 1000)
)

result = (
    grouped
    .select("order_item_quantity_tier", "next_discount_tier", "distinct_payment_status")
)
