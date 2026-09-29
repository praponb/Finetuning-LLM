from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_first(), "id")

deduped = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        "customer_id",
        F.when(F.col("monthly_fee") >= 241, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .filter(F.col("distinct_status") >= 1)
)
