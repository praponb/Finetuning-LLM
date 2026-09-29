from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-03-31").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "age",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

result = (
    cleaned
    .select("id")
    .exceptAll(
        df_customers
        .filter(F.month("signup_date").isin([6, 8, 12]))
        .select("id")
    )
)
