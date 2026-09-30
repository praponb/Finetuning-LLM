from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-03-22").cast("date")) & F.col("segment").isNotNull() & F.col("name").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.substring("segment", 1, 2).alias("segment_prefix"),
        F.concat("country", F.lit("/"), "segment").alias("country_segment"),
    )
)

result = (
    base
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.discount").between(0.15, 0.31)), "left_anti")
)
