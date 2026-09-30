from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.page").isNotNull()), "left_semi")
)

result = (
    eligible
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "CORE").otherwise("EXTENDED").alias("device_zone"),
        F.col("min_order_shipping_fee"),
    )
    .orderBy(F.col("browser").asc_nulls_first())
)
