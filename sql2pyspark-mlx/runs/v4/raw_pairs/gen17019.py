from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("plan", "status").orderBy("start_date", "id")

metrics = (
    df_subscriptions
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w))
    .withColumn("event_seq", F.row_number().over(w_ordered))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("event_seq") >= 7, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
    )
    .agg(
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "ACTIVE", 1)), F.lit(0)), 2).alias("monthly_fee_per_active"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("distinct_customer_ids") >= 1)
    .orderBy("row_count")
)
