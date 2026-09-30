from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") > 50) & F.col("is_active") & ~F.col("segment").isin(["Consumer", "SMB"]))
    .select(
        "id",
        "name",
        (F.col("credit_limit") - F.col("age")).alias("credit_limit_minus_age"),
    )
)
