from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment")

base = (
    df_customers
    .filter((~F.col("city").eqNullSafe(F.lit("New York"))) & F.upper("name").like("%ENR%"))
    .select(
        "*",
        F.coalesce("name", F.lit("unassigned")).alias("name_filled"),
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)

matched_keys = (
    base
    .select("country")
    .intersect(
        df_customers
        .filter(F.col("credit_limit") <= 6100)
        .select("country")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(~F.col("segment").eqNullSafe(F.lit("Corporate")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "email_user",
        F.lower("email").alias("email_lower"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

scored = (
    cleaned
    .filter(F.col("name").like("%Park%"))
    .select(
        "id",
        "email",
        "country",
        "segment",
        "is_active",
        "credit_limit",
        "email_user",
        "email_lower",
        "resolved_city",
        F.corr("age", "credit_limit").over(w).alias("corr_age_credit_limit"),
    )
)

cleaned_customers = (
    scored
    .filter(F.col("email").isNotNull())
    .select(
        "id",
        "segment",
        "is_active",
        "credit_limit",
        "email_user",
        "email_lower",
        "corr_age_credit_limit",
        F.coalesce("corr_age_credit_limit", F.lit(0)).alias("corr_age_credit_limit_filled"),
    )
)

result = (
    cleaned_customers
    .select(
        "id",
        "segment",
        "is_active",
        "email_user",
        "email_lower",
        "corr_age_credit_limit_filled",
    )
)
