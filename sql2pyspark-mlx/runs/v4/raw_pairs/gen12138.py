from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull())
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
        F.length("city").alias("city_length"),
    )
)

result = (
    cleaned
    .select("country", "id")
    .subtract(
        df_customers
        .filter(F.lower(F.trim("email")) == "alice@gmail.com")
        .select("country", "id")
    )
)
