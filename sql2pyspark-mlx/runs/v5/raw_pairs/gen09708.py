from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

base = (
    exploded
    .filter(F.col("customer_id") == 31)
    .select(
        "*",
        F.upper("code").alias("code_upper"),
        F.coalesce("code", F.lit("N/A")).alias("code_clean"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.is_active"))
    .select(
        "a.id",
        "a.customer_id",
        "a.code",
        "a.code_upper",
        "a.code_clean",
        F.col("c.name"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.country"), F.col("c.email"), F.col("c.age"))
)

filtered = (
    joined
    .filter(F.col("code").isNotNull())
    .select("*", F.coalesce("code", F.lit("Unknown")).alias("code_filled"))
)

result = (
    filtered
    .groupBy("customer_id")
    .agg(
        F.min("age").alias("min_age"),
        F.sum(F.when(F.col("code") == "WELCOME5", F.col("age"))).alias("welcome5_total_age"),
    )
    .orderBy("customer_id")
)
