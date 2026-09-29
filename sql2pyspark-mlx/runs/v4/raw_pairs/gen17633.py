from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") <= "2024-03-17") & (F.col("city") == "Singapore") & (F.col("signup_date") <= "2025-09-01"))
    .select(
        "id",
        "is_active",
        "country",
        "name",
        F.regexp_replace("email", " ", "-").alias("email_slug"),
    )
)
