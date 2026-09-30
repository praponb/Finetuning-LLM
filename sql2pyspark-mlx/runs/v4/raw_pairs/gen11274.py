from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "country",
        "name",
        "credit_limit",
        F.round("credit_limit", 0).alias("credit_limit_rounded"),
    )
    .orderBy(F.col("credit_limit").desc())
)
