from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status", "plan").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("monthly_fee", "id")
w4 = Window.partitionBy("status").orderBy("age")
w5 = Window.partitionBy("status").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("plan").orderBy(F.col("age_dense_rank").asc_nulls_last(), "id")

valid = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 90) & F.col("customer_id").isNotNull())
    .withColumn("start_date_plus_months", F.add_months("start_date", 1))
)

joined = (
    valid
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("e.*", F.col("b.age"))
)

metrics = (
    joined
    .filter(F.col("customer_id").isin([16, 20, 26, 28]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.avg("age").over(w_running).alias("age_avg_6"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
        F.ntile(10).over(w3).alias("monthly_fee_bucket"),
        F.dense_rank().over(w4).alias("age_dense_rank"),
        F.last("monthly_fee", ignorenulls=True).over(w5).alias("last_known_monthly_fee"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("YEAR", "start_date").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee_bucket")).otherwise(0)).alias("premium_monthly_fee_bucket"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.min("monthly_fee_bucket").alias("min_monthly_fee_bucket"),
        F.percentile_approx("monthly_fee_bucket", [0.25, 0.5, 0.75]).alias("monthly_fee_bucket_quartiles"),
        F.count("*").alias("cnt"),
        F.max(F.when(F.col("status") == "PAUSED", F.col("age_dense_rank"))).alias("paused_max_age_dense_rank"),
    )
    .orderBy(F.col("min_monthly_fee_bucket").desc_nulls_last())
)
