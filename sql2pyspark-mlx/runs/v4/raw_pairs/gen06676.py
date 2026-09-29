from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("is_cash").desc_nulls_first(), "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-10-08 04:00:00"))) & F.col("order_id").isNotNull())
    .withColumn("is_cash", F.when(F.col("method") == "cash", 1).otherwise(0))
)

joined = (
    base
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.amount").alias("order_amount"),
        F.col("d.shipping_fee"),
        F.col("d.discount"),
    )
)

base_payments = (
    joined
    .filter(F.col("paid_at") > F.lit("2023-09-20 13:00:00").cast("timestamp"))
    .select("*", F.coalesce("order_amount", F.lit(0)).alias("order_amount_filled"))
)

deduped = (
    base_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

flagged = (
    deduped
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w2).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_burst").over(w3))

result = (
    sessionized
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "is_cash",
        "shipping_fee",
        "discount",
        "order_amount_filled",
        "row_num",
        F.when(F.col("method").rlike("^(promptpay|paypal)$"), "PRIORITY").when(F.col("method").rlike("^(card|bank_transfer)$"), "OTHER_ZONE").otherwise("CORE").alias("method_zone"),
    )
    .orderBy(F.col("method_zone").desc())
)
