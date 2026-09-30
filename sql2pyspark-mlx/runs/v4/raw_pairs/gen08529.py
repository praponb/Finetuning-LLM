from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-05-27").cast("date"))
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
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

result = (
    cleaned
    .groupBy("segment", "city")
    .pivot("country", ["DE", "UK"])
    .agg(F.max("age").alias("total"))
)
