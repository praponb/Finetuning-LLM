from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_semi")
)

result = (
    eligible
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.when(F.col("device").rlike("^(tablet|desktop)$"), "CORE").when(F.col("device").rlike("^(mobile)$"), "PRIORITY").otherwise("EXTENDED").alias("device_zone"),
        F.col("avg_order_discount"),
    )
    .orderBy(F.col("device").desc_nulls_first())
)
