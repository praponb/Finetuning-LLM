from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "SMB"]))
    .select(
        "id",
        "credit_limit",
        "email",
        F.lower("country").alias("country_lower"),
        F.length("name").alias("name_length"),
    )
    .orderBy(F.col("name_length").desc())
)
