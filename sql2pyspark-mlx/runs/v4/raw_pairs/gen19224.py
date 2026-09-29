from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "US") & (F.col("segment") == "Corporate") & F.col("segment").isin(["SMB", "Consumer", "Corporate"]))
    .select(
        "id",
        "segment",
        "age",
        "credit_limit",
        F.col("age").cast("string").alias("age_str"),
    )
)
