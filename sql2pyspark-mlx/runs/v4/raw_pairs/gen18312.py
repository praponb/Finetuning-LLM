from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("plan").orderBy("monthly_fee", "id")

metrics = (
    df_subscriptions
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w_recent))
    .withColumn("customer_id_stddev_samp_monthly_fee", F.stddev_samp("monthly_fee").over(w2))
    .withColumn("monthly_fee_avg_5_prior", F.avg("monthly_fee").over(w3))
    .withColumn("monthly_fee_bucket", F.ntile(4).over(w4))
)

result = (
    metrics
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "running_monthly_fee",
        "customer_id_stddev_samp_monthly_fee",
        "monthly_fee_avg_5_prior",
    )
)
