from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

enriched = (
    df_payments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.order_date"))
)

remaining_keys = (
    enriched
    .select("status")
    .subtract(
        df_payments
        .filter(F.col("method") == "promptpay")
        .select("status")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.stddev_samp("amount").over(w).alias("method_stddev_samp_amount"),
        F.percent_rank().over(w2).alias("amount_percent_rank"),
        F.rank().over(w3).alias("amount_rank"),
    )
)

result = (
    scored
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "paid_at",
        "status",
        "order_date",
        "amount_percent_rank",
        "amount_rank",
        F.col("max_payment_amount"),
    )
    .orderBy("order_id")
)
