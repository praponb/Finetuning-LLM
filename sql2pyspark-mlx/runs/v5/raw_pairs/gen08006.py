from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "is_active",
        "age",
        (F.col("credit_limit") - F.col("age")).alias("credit_limit_minus_age"),
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
