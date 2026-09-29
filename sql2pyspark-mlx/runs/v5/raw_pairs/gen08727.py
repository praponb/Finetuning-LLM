from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "country",
        "segment",
        F.round((F.col("age") / F.col("credit_limit")), 2).alias("age_per_credit_limit"),
    )
)
