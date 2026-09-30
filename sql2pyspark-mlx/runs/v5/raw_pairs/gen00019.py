from pyspark.sql import functions as F

valid = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-04-25").cast("date"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.trim("name").alias("clean_name"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_clean"),
    )
)

result = (
    valid
    .select("country")
    .intersect(
        df_customers
        .filter(F.col("country").isin(["UK", "US"]))
        .select("country")
    )
)
