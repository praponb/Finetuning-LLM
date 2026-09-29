from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
        F.to_date("paid_at").alias("paid_at_date"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
