from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

enriched = (
    lines
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.segment"), F.col("c.country"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.email"), F.col("d.signup_date"))
)

enriched_carts = (
    with_dims
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("a.customer_id", "a.created_at", "a.element", F.col("b.credit_limit"))
)

scored = (
    enriched_carts
    .filter(F.col("element") != "WELCOME5")
    .select(
        "customer_id",
        "created_at",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
    )
)

result = (
    scored
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "created_at", F.col("max_account_balance"))
    .orderBy("created_at", F.col("customer_id").desc())
)
