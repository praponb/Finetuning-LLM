from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date")

deduped = (
    df_subscriptions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("week", "start_date").alias("week_start"))
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
    .filter(F.col("cancelled_count") < 1000)
    .orderBy(F.col("week_start").desc())
)
