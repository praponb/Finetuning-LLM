from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")

valid = (
    df_customers
    .filter((F.col("age") != 42) & (~F.col("segment").eqNullSafe(F.lit("Consumer"))))
    .withColumn("country_title", F.initcap(F.trim("country")))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") >= F.col("s.signup_date")), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-08-16")))
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 15000, "High").when(F.col("credit_limit") >= 6400, "Medium").otherwise("Low"))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("UNKNOWN")))
)

kept = (
    filtered
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.customer_id").isin([8, 10, 16])), "left_anti")
)

valid_customers = (
    kept
    .filter((F.col("signup_date") > F.lit("2023-08-27").cast("date")) & (F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "age",
        "country_title",
        "credit_limit_tier",
        "resolved_segment",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

latest = (
    valid_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "name", "email", "age", "country_title", "resolved_segment")
)

result = (
    latest
    .select("id", "name", "age", "country_title")
    .orderBy(F.col("age").desc_nulls_first(), "id")
)
