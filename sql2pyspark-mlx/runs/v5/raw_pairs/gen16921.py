from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~(F.col("country").isin(["SG", "US"]) & ((F.year("signup_date") >= 2024) | (F.col("city") == "Tokyo"))))
    .select(
        "age",
        "segment",
        F.lower("segment").alias("segment_lower"),
        F.lower("segment").alias("segment_lower_2"),
    )
)
