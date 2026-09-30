from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "credit_limit",
        "is_active",
        F.concat_ws("-", "country", "name").alias("country_name_key"),
    )
    .orderBy(F.col("country_name_key").desc())
)
