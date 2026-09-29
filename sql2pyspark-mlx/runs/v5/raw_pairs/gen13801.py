from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc_nulls_last(), "id")

deduped = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy("customer_id")
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.countDistinct("status").alias("distinct_status"),
        F.countDistinct("status").alias("distinct_status_2"),
    )
    .filter(F.col("expired_monthly_fee") > 0)
    .orderBy(F.col("expired_monthly_fee").asc_nulls_last())
)
