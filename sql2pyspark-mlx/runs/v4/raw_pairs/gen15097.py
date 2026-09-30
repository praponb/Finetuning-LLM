from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "is_active",
        "segment",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
    .orderBy(F.col("is_active_flag").desc())
)
