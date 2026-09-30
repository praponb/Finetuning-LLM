from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "DE") & (~F.col("is_active") | ~F.col("segment").isin(["Consumer", "SMB"])))
    .select(
        "city",
        "age",
        "name",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
    )
)
