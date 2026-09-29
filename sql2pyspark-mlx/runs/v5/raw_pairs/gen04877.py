from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

lines_carts = (
    lines
    .select(
        "id",
        "customer_id",
        "k",
        "attr_value",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

joined = (
    lines_carts
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.segment") == "SMB")
    .select("src.*", F.col("p.country"), F.col("p.credit_limit"), F.col("p.city"))
)

valid = (
    joined
    .filter(F.col("city").isNotNull() & F.col("credit_limit").between(5000, 10100))
    .select(
        "id",
        "customer_id",
        "k",
        "tag",
        "country",
        "credit_limit",
        "city",
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.city").alias("customer_city"),
        F.col("d.segment").alias("segment"),
        F.col("e.tag").alias("tag"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("e.credit_limit")), 2).alias("total_credit_limit"),
    )
)
