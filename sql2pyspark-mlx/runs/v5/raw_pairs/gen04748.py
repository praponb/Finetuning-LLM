from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("method").orderBy(F.col("paid_at_hour_rounded").desc_nulls_last())

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("status") == "REFUNDED"))
    .select("id", "method", "amount", "status", F.hour("paid_at").alias("paid_at_hour"))
)

filtered = (
    cleaned
    .filter(F.col("paid_at_hour").between(53, 71))
    .withColumn("method_group", F.when(F.col("method").isin(["paypal", "bank_transfer"]), "Group A").otherwise("Other"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("N/A")))
)

filtered_payments = (
    filtered
    .filter((~F.col("method").eqNullSafe(F.lit("promptpay"))) & (F.col("method") == "cash"))
    .withColumn("status_upper", F.upper("status"))
    .withColumn("paid_at_hour_rounded", F.round("paid_at_hour", 2))
)

enriched = (
    filtered_payments
    .filter(F.col("amount").isNotNull())
    .withColumn("amount_avg_6_prior", F.avg("amount").over(w))
    .withColumn("paid_at_hour_rounded_percent_rank", F.percent_rank().over(w2))
)

result = (
    enriched
    .select(
        "id",
        "amount",
        "status",
        "paid_at_hour",
        "resolved_method",
        "paid_at_hour_rounded",
        "amount_avg_6_prior",
    )
)
