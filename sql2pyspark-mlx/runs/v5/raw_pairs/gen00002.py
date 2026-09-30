from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-05-01").cast("date"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("city").isin(["Berlin", "Singapore"]), "Core").otherwise("Other").alias("city_group"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_clean"),
    )
)

result = (
    base
    .groupBy("city", "segment")
    .pivot("country", ["DE", "TH"])
    .agg(F.sum("age"))
    .select("city", "segment", F.col("DE").alias("de"), F.col("TH").alias("th"))
)
