from pyspark.sql import functions as F

base = df_payments.filter(F.col("order_id") == 28)

payments_stats = (
    base
    .groupBy(
        F.when(F.col("method").isin(["card", "promptpay"]), "core").otherwise("other").alias("method_group"),
    )
    .agg(F.max("paid_at").alias("last_paid_at"))
)

result = (
    payments_stats
    .select("method_group", "last_paid_at")
    .orderBy("method_group", "last_paid_at")
)
