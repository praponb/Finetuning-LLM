from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "method",
        F.substring("status", 1, 3).alias("status_prefix"),
        F.trim("status").alias("clean_status"),
    )
    .orderBy("clean_status")
)
