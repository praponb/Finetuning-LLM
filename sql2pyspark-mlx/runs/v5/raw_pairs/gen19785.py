from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("signup_date") >= "2023-11-01")
    .select(
        "*",
        F.date_sub("signup_date", 66).alias("signup_date_minus_66d"),
        F.concat("name", F.lit(", "), "segment").alias("name_segment"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.end_date") > "2025-03-10"), "left_semi")
)
