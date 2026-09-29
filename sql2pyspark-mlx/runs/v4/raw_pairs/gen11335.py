from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("plan").orderBy("start_date", "id")
w3 = Window.partitionBy("status").orderBy("monthly_fee", "id")

enriched = (
    df_subscriptions
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("cumulative_monthly_fee"),
        F.lag("monthly_fee", 1).over(w_part).alias("prev_monthly_fee"),
        F.ntile(4).over(w3).alias("monthly_fee_bucket"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("QUARTER", "end_date").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
        F.min("cumulative_monthly_fee").alias("min_cumulative_monthly_fee"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.count_if(F.col("status") == "EXPIRED").alias("expired_events"),
    )
    .orderBy("num_plans")
)
