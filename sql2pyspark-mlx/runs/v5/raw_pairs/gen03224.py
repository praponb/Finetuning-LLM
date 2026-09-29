from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter(F.col("city").isin(["Bangkok", "London"]) & (F.col("signup_date") >= F.to_date(F.lit("2023-08-21"))))
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
        F.trim("name").alias("clean_name"),
    )
)

result = (
    cleaned
    .groupBy("city")
    .pivot("country", ["SG", "JP", "TH"])
    .agg(F.avg("age").alias("amount"))
    .select(
        "city",
        F.col("SG").alias("sg"),
        F.col("JP").alias("jp"),
        F.col("TH").alias("th"),
    )
)
