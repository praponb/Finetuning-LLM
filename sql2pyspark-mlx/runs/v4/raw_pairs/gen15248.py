from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") != 43)
    .select(
        "id",
        "segment",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
