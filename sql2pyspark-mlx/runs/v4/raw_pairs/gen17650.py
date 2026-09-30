from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-11-06").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
        F.when(F.col("city") == "Bangkok", 1).otherwise(0).alias("is_bangkok"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_filled"),
    )
)

result = (
    filtered
    .select("country", "id")
    .intersect(
        df_customers
        .filter(F.length("name") > 9)
        .select("country", "id")
    )
)
