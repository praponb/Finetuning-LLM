from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") | (F.length("email") < 11))
    .select(
        "is_active",
        F.upper("segment").alias("segment_upper"),
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
    )
)
