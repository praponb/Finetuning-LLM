from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "is_active",
        "email",
        "credit_limit",
        F.round("age", 2).alias("age_rounded"),
    )
    .orderBy(F.col("age_rounded").desc())
)
