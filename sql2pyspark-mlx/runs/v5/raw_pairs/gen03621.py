from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

with_dims = (
    df_orders
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.city") == "London")
    .select("s.*", F.col("p.sqft"))
)

latest = (
    with_dims
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "store_id", "order_date", "amount")
)

latest_match = (
    latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

with_window = (
    latest_match
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.order_date") >= F.col("sub.start_date")) & (F.col("a.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched = (
    with_window
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.store_id",
        "a.subscription_id",
        "a.monthly_fee",
        F.col("b.store_name"),
        F.col("b.region"),
        F.col("b.city"),
    )
)

result = (
    enriched
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "subscription_id",
        "monthly_fee",
        "store_name",
        "region",
        "city",
        F.when(F.col("region").rlike("^(North|Central)$"), "CORE").otherwise("OTHER_ZONE").alias("region_zone"),
        F.col("min_account_balance"),
    )
    .orderBy(F.col("subscription_id").desc_nulls_first(), "monthly_fee")
)
