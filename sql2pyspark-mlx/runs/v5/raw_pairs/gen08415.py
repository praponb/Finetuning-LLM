from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") <= 15600)
    .select(
        "age",
        "email",
        "name",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
