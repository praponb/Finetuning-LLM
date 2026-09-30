from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("tag", "city")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

cleaned = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-01 17:00:00").cast("timestamp")) & F.col("tag").isin(["VIP", "WELCOME5", "BUNDLE"]))
    .select(
        "*",
        F.to_date("created_at").alias("created_at_date"),
        F.date_trunc("month", "created_at").alias("created_at_period"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "tag",
        "created_at_date",
        "created_at_period",
        F.add_months("created_at_date", 6).alias("created_at_date_plus_months"),
        F.date_trunc("year", "created_at_period").alias("created_at_period_period"),
    )
)

joined = (
    cleaned_carts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.city"), F.col("c.credit_limit"), F.col("c.name"))
)

metrics = (
    joined
    .select(
        "id",
        "customer_id",
        "created_at_date",
        "created_at_period",
        "created_at_date_plus_months",
        "created_at_period_period",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.min("credit_limit").over(w2).alias("tag_min_credit_limit"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.when(F.col("credit_limit_share") >= 0.68, "PREMIUM").otherwise("STANDARD").alias("credit_limit_share_tier"),
        "customer_id",
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at_period")) - F.unix_timestamp(F.min("created_at_period"))) / 60).alias("duration_min"),
        F.max("created_at_period_period").alias("last_seen"),
        F.max("created_at_period").alias("last_created_at_period"),
    )
)

result = (
    grouped
    .select(
        "credit_limit_share_tier",
        "customer_id",
        "duration_min",
        "last_seen",
        "last_created_at_period",
    )
)
