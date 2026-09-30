from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(-3, -1)
w_ordered = Window.orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")

metrics = (
    df_accounts
    .withColumn("prev_balance", F.lag("balance", 2).over(w_running))
    .withColumn("balance_avg_3_prior", F.avg("balance").over(w2))
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("balance_bucket", F.ntile(10).over(w4))
)

result = (
    metrics
    .rollup(
        F.when(F.col("balance_avg_3_prior") >= 70558, "PREMIUM").otherwise("STANDARD").alias("balance_avg_3_prior_tier"),
        "seq_no",
    )
    .agg(
        F.round(F.sum("balance_avg_3_prior") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_avg_3_prior_per_closed"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_rows"),
        F.round(F.sum("balance_bucket") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_bucket_per_closed"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.avg(F.when(F.col("status") == "FROZEN", F.col("balance_bucket"))).alias("frozen_avg_balance_bucket"),
    )
    .orderBy(F.col("balance_avg_3_prior_tier").asc_nulls_first())
)
