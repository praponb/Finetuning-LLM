from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.hour("paid_at").alias("paid_at_hour"))
)

result = (
    valid
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.order_date"),
        F.col("p.status").alias("order_status"),
        F.col("p.amount").alias("order_amount"),
    )
)
