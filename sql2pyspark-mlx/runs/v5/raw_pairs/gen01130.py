from pyspark.sql import functions as F

revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

kept_keys = (
    df_orders
    .select("channel")
    .intersectAll(
        df_orders
        .filter(F.col("customer_id") == 25)
        .select("channel")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        F.when(F.col("status").rlike("^(COMPLETED|SHIPPED)$"), "EXTENDED").when(F.col("status").rlike("^(PENDING|CANCELLED)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("status_zone"),
        F.col("sum_store_sale_revenue"),
    )
    .orderBy(F.col("order_date").asc_nulls_first())
)
