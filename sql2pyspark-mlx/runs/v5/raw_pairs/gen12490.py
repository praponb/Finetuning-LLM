from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") < 52) & F.month("signup_date").isin([1, 6, 8]))
    .select(
        "id",
        "credit_limit",
        "is_active",
        "city",
        F.least("credit_limit", "age").alias("least_credit_limit_age"),
        F.when(F.col("city") == "Tokyo", 1).otherwise(0).alias("is_tokyo"),
    )
)
