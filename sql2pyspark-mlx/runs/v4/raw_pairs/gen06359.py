from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "country",
        "segment",
        F.quarter("signup_date").alias("signup_date_quarter"),
        F.lower("segment").alias("segment_lower"),
    )
    .orderBy("segment_lower", F.col("country").desc())
)
