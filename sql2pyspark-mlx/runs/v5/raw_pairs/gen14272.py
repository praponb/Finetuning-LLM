from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Hen%"))
    .select(
        "id",
        "is_active",
        "segment",
        F.greatest("credit_limit", "age").alias("greatest_credit_limit_age"),
    )
)
