from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-11-23")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        F.ceil("credit_limit").alias("credit_limit_whole"),
        F.length("country").alias("country_length"),
    )
)

base = (
    filtered
    .filter((F.col("signup_date") >= F.lit("2023-09-22").cast("date")) & F.col("segment").isNotNull())
    .withColumn("credit_limit_whole_filled", F.coalesce("credit_limit_whole", F.lit(0)))
    .withColumn("city_clean", F.coalesce("city", F.lit("UNKNOWN")))
)

with_latest = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"), F.col("c.order_date").alias("order_order_date"))
)

kept_keys = (
    with_latest
    .select("segment", "id")
    .subtract(
        df_customers
        .filter(F.col("credit_limit") < 13400)
        .select("segment", "id")
    )
)

verified = (
    with_latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit_whole",
        "country_length",
        "credit_limit_whole_filled",
        "order_order_date",
    )
    .orderBy(F.col("signup_date").asc_nulls_first(), "country_length")
)
