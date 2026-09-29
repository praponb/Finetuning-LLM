from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-08-31").cast("date"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.concat_ws(",", "city", "country").alias("city_country_key"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_filled"),
    )
)

result = (
    base
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.discount") <= 0.3), "left_semi")
)
