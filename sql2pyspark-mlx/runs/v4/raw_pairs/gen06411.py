from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(11900, 15200))
    .select(
        "id",
        "email",
        "credit_limit",
        F.length("country").alias("country_length"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
    .orderBy(F.col("email").desc())
)
