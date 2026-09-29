from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-02-18").cast("date"))
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
        F.concat_ws("-", "name", "city").alias("name_city_key"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.segment",
        "s.is_active",
        "s.age",
        F.col("c.monthly_fee"),
        F.col("c.end_date"),
    )
)

filtered = (
    latest_match
    .filter(F.col("city").isNotNull() & (F.col("segment").eqNullSafe(F.lit("Consumer"))))
    .select(
        "id",
        "country",
        "city",
        "is_active",
        "monthly_fee",
        "end_date",
        F.upper("segment").alias("segment_upper"),
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") < F.col("s.end_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "country",
        "city",
        "is_active",
        "city_title",
        F.when(F.col("city").rlike("^(Chiang Mai|New York)$"), "CORE").otherwise("GROUP_A").alias("city_zone"),
    )
)
