from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "country",
        "age",
        "segment",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
