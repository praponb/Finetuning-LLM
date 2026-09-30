from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

lines = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

cleaned = (
    lines
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.coalesce("code", F.lit("Unknown")).alias("code_filled"),
        F.upper("code").alias("code_upper"),
        F.coalesce("code", F.lit("OTHER")).alias("code_clean"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.code",
        "a.code_filled",
        "a.code_upper",
        "a.code_clean",
        F.col("d.credit_limit"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.age"), F.col("c.name"))
)

cleaned_carts = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "code_filled",
        "code_upper",
        "code_clean",
        "credit_limit",
        "age",
        F.concat("name", F.lit("/"), "code").alias("name_code"),
    )
)

grouped = (
    cleaned_carts
    .groupBy(
        F.when(F.col("age") >= 58, "HIGH").otherwise("LOW").alias("age_tier"),
        "customer_id",
    )
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "age_tier",
        "customer_id",
        "avg_credit_limit",
        "row_count",
        F.col("min_account_balance"),
    )
)
