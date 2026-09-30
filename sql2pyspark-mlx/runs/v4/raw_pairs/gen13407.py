from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & F.col("segment").isin(["Corporate", "Consumer", "SMB"]))
    .select(
        "id",
        "email",
        "signup_date",
        "segment",
        F.when(F.col("city") == "Bangkok", 1).otherwise(0).alias("is_bangkok"),
    )
)
