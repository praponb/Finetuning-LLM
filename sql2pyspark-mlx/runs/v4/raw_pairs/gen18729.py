from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@startup.io"))
    .select(
        "email",
        "segment",
        "city",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
