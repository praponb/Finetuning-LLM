from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

joined = (
    df_sessions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.device",
        "src.browser",
        "src.started_at",
        F.col("p.country").alias("customer_country"),
        F.col("p.age"),
    )
)

result = (
    joined
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "age",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "EXTENDED").when(F.col("device").rlike("^(tablet)$"), "OTHER_ZONE").otherwise("CORE").alias("device_zone"),
        F.col("sum_order_amount"),
    )
    .orderBy(F.col("id").desc_nulls_last())
    .limit(5)
)
