from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("amount", "id")

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        F.when(F.col("shipping_fee") >= 41, "High").when(F.col("shipping_fee") >= 11, "Medium").otherwise("Low").alias("shipping_fee_level"),
        F.upper("channel").alias("channel_upper"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.lead("amount").over(w_recent).alias("next_amount"),
        F.lag("amount").over(w_recent).alias("prev_amount"),
        F.lag("shipping_fee").over(w_recent).alias("prev_shipping_fee"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.status").isin(["SUCCESS", "FAILED", "REFUNDED"])), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") < F.col("s.order_date")), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "shipping_fee",
        "shipping_fee_level",
        "next_amount",
        "prev_shipping_fee",
        "row_seq",
        F.when(F.col("channel").rlike("^(web|mobile)$"), "EXTENDED").when(F.col("channel").rlike("^(store|partner)$"), "CORE").otherwise("PRIORITY").alias("channel_zone"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last())
)
