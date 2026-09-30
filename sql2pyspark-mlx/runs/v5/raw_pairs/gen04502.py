from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.year("signup_date") == 2024)
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
    .withColumn("country_length", F.length("country"))
)

matched_keys = (
    filtered
    .select("city")
    .intersect(
        df_customers
        .filter(F.col("credit_limit").between(4400, 7200))
        .select("city")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("country_length") > 24)
    .select(
        "*",
        F.greatest("is_active_flag", "age").alias("greatest_is_active_flag_age"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.status").isin(["OPEN", "CLOSED"])), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") < F.col("s.signup_date")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "is_active_flag",
        F.when(F.col("city").rlike("^(New York|Berlin)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("CORE").alias("city_zone"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
