from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status").orderBy(F.col("monthly_fee").asc_nulls_first())

metrics = (
    df_subscriptions
    .withColumn("customer_id_stddev_samp_monthly_fee", F.stddev_samp("monthly_fee").over(w))
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w2))
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w_grp))
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        "customer_id_stddev_samp_monthly_fee",
    )
    .orderBy(
        F.col("customer_id_stddev_samp_monthly_fee").desc_nulls_first(),
        F.col("status").desc(),
    )
)
