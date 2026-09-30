from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("method", "order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("amount")
w5 = Window.orderBy(F.col("amount").desc(), "id")

base = (
    df_payments
    .filter((~F.col("method").eqNullSafe(F.lit("promptpay"))) & (F.col("method") != "promptpay"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("amount") >= 1260, "High").when(F.col("amount") >= 650, "Medium").otherwise("Low").alias("amount_band"),
        F.upper("status").alias("status_upper"),
    )
)

scored = (
    base
    .filter(F.col("status") == "REFUNDED")
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("amount_avg_3", F.avg("amount").over(w2))
    .withColumn("amount_count_30d", F.count("amount").over(w3))
    .withColumn("amount_cume_dist", F.cume_dist().over(w4))
    .withColumn("amount_bucket", F.ntile(3).over(w5))
)

joined = (
    scored
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.order_date"),
        F.col("b.shipping_fee"),
        F.col("b.amount").alias("order_amount"),
    )
)

matched_keys = (
    joined
    .select("status")
    .intersectAll(
        df_payments
        .filter(F.col("status") == "SUCCESS")
        .select("status")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("shipping_fee") <= 11)
    .select("*", F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"))
)

result = (
    valid
    .select(
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_band",
        "status_upper",
        "running_amount",
        "amount_avg_3",
        F.when(F.col("method").rlike("^(paypal|cash)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("method_zone"),
    )
)
