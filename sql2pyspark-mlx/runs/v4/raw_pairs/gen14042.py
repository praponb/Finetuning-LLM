from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

flattened = (
    exploded
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

enriched = (
    flattened
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.email"), F.col("d.city"))
)

enriched_carts = (
    enriched
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.segment"), F.col("b.age"))
)

scored = (
    enriched_carts
    .select(
        "*",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.avg("age").over(w).alias("element_avg_age"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select(
        "a.*",
        F.col("d.age").alias("customer_age"),
        F.col("d.country"),
        F.col("d.email").alias("customer_email"),
    )
)

result = (
    joined
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "element",
        "email",
        "city",
        "segment",
        "age",
        "element_avg_age",
        "customer_age",
        "country",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("min_account_balance").desc_nulls_first())
)
