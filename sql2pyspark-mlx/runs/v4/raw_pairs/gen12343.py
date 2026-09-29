from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-06-05"))) & F.col("country").isin(["UK", "DE", "JP"]))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_filled"),
    )
)

matched_keys = (
    cleaned
    .select("city", "country")
    .subtract(
        df_customers
        .filter(F.col("age") > 35)
        .select("city", "country")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

cleaned_customers = (
    verified
    .filter(F.col("city").isNotNull())
    .select(
        "name",
        "email",
        "country",
        "city",
        "segment",
        "age",
        "is_active_label",
        "segment_filled",
        F.concat("segment", F.lit("/"), "country").alias("segment_country"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

valid = (
    cleaned_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "name",
        "email",
        "city",
        "segment",
        "age",
        "is_active_label",
        "segment_filled",
        "city_filled",
        F.trim("country").alias("clean_country"),
        F.ceil("age").alias("age_whole"),
    )
)

result = (
    valid
    .select(
        "name",
        "city",
        "age",
        "is_active_label",
        "segment_filled",
        "city_filled",
        "clean_country",
        "age_whole",
        F.when(F.col("city").rlike("^(Chiang Mai|Berlin)$"), "CORE").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("age_whole").asc_nulls_first())
)
