from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")

deduped = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .withColumn("year_start", F.date_trunc("YEAR", "start_date"))
    .groupingSets([["status", "year_start"], ["status"], []], "status", "year_start")
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
    .orderBy("year_start")
)
