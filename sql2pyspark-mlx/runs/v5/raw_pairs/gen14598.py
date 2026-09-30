from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

base = (
    lines
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & (~F.col("tag").eqNullSafe(F.lit("SPRING10"))))
    .withColumn("tag_title", F.initcap(F.trim("tag")))
    .withColumn("tag_filled", F.coalesce("tag", F.lit("DIRECT")))
)

valid = (
    base
    .filter(F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 22:00:00")))
    .select(
        "id",
        "customer_id",
        "created_at",
        "tag",
        "tag_title",
        F.when(F.col("tag").isin(["SPRING10", "VIP"]), "Tier 1").otherwise("Other").alias("tag_group"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city") != "Chiang Mai")
    .select("a.*", F.col("c.signup_date"), F.col("c.city"))
)

result = (
    with_dims
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "tag_group",
        "signup_date",
        "city",
        F.col("sum_account_balance"),
    )
)
