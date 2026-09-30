from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "customer_id")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("segment").orderBy("age")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

joined = (
    flattened
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.created_at",
        "s.element",
        F.col("b.name"),
        F.col("b.segment"),
        F.col("b.email"),
    )
)

enriched = (
    joined
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.email").alias("customer_email"), F.col("b.age"))
)

with_windows = (
    enriched
    .filter(~F.col("email").like("%@startup.io"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "name",
        "segment",
        "customer_email",
        "age",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share_2"),
        F.cume_dist().over(w3).alias("age_cume_dist"),
    )
)

result = (
    with_windows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "created_at",
        "name",
        "segment",
        "customer_email",
        "age",
        "age_share",
        "age_share_2",
        "age_cume_dist",
        F.col("avg_account_balance"),
    )
    .orderBy("created_at")
)
