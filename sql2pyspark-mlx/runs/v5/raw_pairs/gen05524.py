from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "name",
        "age",
        "country",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)
