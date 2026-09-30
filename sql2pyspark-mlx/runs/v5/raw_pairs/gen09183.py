from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.promo"),
    )
)

lines = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "promo",
        F.explode("coupon_codes").alias("element"),
    )
)

joined = (
    lines
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .filter(F.year(F.col("d.signup_date")) == 2024)
    .select("a.*", F.col("d.age"))
)

enriched = (
    joined
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.customer_id",
        "e.created_at",
        "e.promo",
        "e.element",
        F.col("b.signup_date"),
        F.col("b.email"),
        F.col("b.name"),
    )
)

base = (
    enriched
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .select(
        "customer_id",
        "created_at",
        "promo",
        "element",
        "signup_date",
        "email",
        "name",
        F.lower("email").alias("email_lower"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_clean"),
    )
)

result = (
    base
    .select(
        "customer_id",
        "created_at",
        "promo",
        "signup_date",
        "name",
        "email_lower",
        "promo_clean",
        F.when(F.col("name").rlike("^(Jack Taylor|Pim Chaiyaporn)$"), "EXTENDED").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("name_zone"),
    )
    .orderBy(F.col("name_zone").desc())
)
