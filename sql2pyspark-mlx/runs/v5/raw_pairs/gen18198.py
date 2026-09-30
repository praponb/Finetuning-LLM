from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

kept_keys = (
    df_sessions
    .select("browser")
    .exceptAll(
        df_sessions
        .filter(F.to_date("started_at") >= "2024-01-01")
        .select("browser")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.col("avg_order_discount"),
    )
    .orderBy(F.col("browser").desc())
)
