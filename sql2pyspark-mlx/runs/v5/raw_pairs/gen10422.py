from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

cleaned = (
    flattened
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("element").isNotNull())
    .select(
        "*",
        F.upper("element").alias("element_upper"),
        F.length("element").alias("element_length"),
        F.coalesce("element", F.lit("NONE")).alias("element_filled"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.created_at",
        "e.element_upper",
        "e.element_length",
        "e.element_filled",
        F.col("b.email"),
        F.col("b.credit_limit"),
    )
)

enriched_carts = (
    enriched
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.customer_id",
        "e.element_upper",
        "e.element_filled",
        F.col("p.signup_date"),
        F.col("p.country"),
        F.col("p.email").alias("customer_email"),
    )
)

valid = (
    enriched_carts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "element_upper",
        "element_filled",
        "signup_date",
        "country",
        "customer_email",
        F.upper("customer_email").alias("customer_email_upper"),
    )
)

result = (
    valid
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .groupingSets(
        [["quarter_start", "country"], ["quarter_start"]],
        "quarter_start",
        "country",
    )
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.countDistinct("country").alias("distinct_countries"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.grouping_id().alias("gid"),
    )
)
