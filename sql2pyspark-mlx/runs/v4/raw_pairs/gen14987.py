from pyspark.sql import functions as F

valid = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-11-17"))) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        F.col("is_active").cast("int").alias("is_active_flag"),
        F.date_add("signup_date", 73).alias("signup_date_plus_73d"),
    )
)

result = (
    valid
    .groupBy("segment")
    .pivot("city", ["Singapore", "Berlin", "London"])
    .agg(F.sum("is_active_flag").alias("amount"), F.count(F.lit(1)).alias("n"))
)
