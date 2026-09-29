from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("method", "order_id")
w3 = Window.partitionBy("status", "channel").orderBy(F.col("shipping_fee").desc_nulls_first())

joined = (
    df_payments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.channel"),
        F.col("p.amount").alias("order_amount"),
        F.col("p.shipping_fee"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("paid_at") >= F.lit("2023-09-24").cast("date"))
    .select(
        "id",
        "method",
        "amount",
        "status",
        "channel",
        "shipping_fee",
        "row_seq",
        F.when(F.col("amount") >= 1790, "High").when(F.col("amount") >= 750, "Medium").otherwise("Low").alias("amount_level"),
        (F.col("amount_share") - F.col("order_amount")).alias("amount_share_minus_order_amount"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "method",
        "channel",
        "shipping_fee",
        "row_seq",
        "amount_level",
        F.when(F.col("method").rlike("^(card|bank_transfer)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("method_zone"),
    )
)
