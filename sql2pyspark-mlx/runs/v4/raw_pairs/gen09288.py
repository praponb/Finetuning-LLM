from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_first())
w3 = Window.partitionBy("status").orderBy(F.col("monthly_fee_dense_rank").desc(), "id")

metrics = (
    df_subscriptions
    .withColumn("monthly_fee_dense_rank", F.dense_rank().over(w))
    .withColumn("monthly_fee_rank", F.rank().over(w2))
)

enriched = (
    metrics
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.age"))
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .cube(F.date_trunc("QUARTER", "end_date").alias("quarter_start"))
    .agg(
        F.max(F.when(F.col("status") == "PAUSED", F.col("monthly_fee_dense_rank"))).alias("paused_max_monthly_fee_dense_rank"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee_dense_rank")).otherwise(0)).alias("active_monthly_fee_dense_rank"),
        F.sum(F.when(F.col("status") == "PAUSED", F.col("age")).otherwise(0)).alias("paused_age"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
    )
)
