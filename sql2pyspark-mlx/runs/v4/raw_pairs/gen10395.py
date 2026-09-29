from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

joined = (
    exploded
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.signup_date"), F.col("d.name"), F.col("d.country"))
)

joined_carts = (
    joined
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.city"), F.col("c.credit_limit"), F.col("c.age"))
)

with_dims = (
    joined_carts
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.credit_limit").alias("customer_credit_limit"),
        F.col("b.age").alias("customer_age"),
        F.col("b.country").alias("customer_country"),
    )
)

result = (
    with_dims
    .select(
        "code",
        "signup_date",
        "name",
        "country",
        "city",
        "credit_limit",
        "customer_credit_limit",
        F.when(F.col("country").rlike("^(DE|SG)$"), "GROUP_B").when(F.col("country").rlike("^(TH|US)$"), "PRIORITY").otherwise("EXTENDED").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc_nulls_last())
)
