from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("monthly_fee").asc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id", "plan").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_first())

base = (
    df_subscriptions
    .filter((F.col("start_date") > F.to_date(F.lit("2023-07-13"))) & F.col("plan").isin(["enterprise", "basic"]))
    .withColumn("status_plan", F.concat("status", F.lit("/"), "plan"))
    .withColumn("clean_status", F.trim("status"))
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "plan", "start_date", "monthly_fee", "status")
)

enriched = (
    latest
    .withColumn("monthly_fee_avg_6", F.avg("monthly_fee").over(w2))
    .withColumn("monthly_fee_bucket", F.ntile(5).over(w3))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w4))
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .withColumn("week_start", F.date_trunc("WEEK", "start_date"))
    .withColumn("activity_month", F.date_trunc("MONTH", "start_date"))
    .groupingSets(
        [["week_start", "activity_month"], ["week_start"]],
        "week_start",
        "activity_month",
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.avg("monthly_fee_avg_6"), 2).alias("avg_monthly_fee_avg_6"),
    )
    .filter(F.col("basic_count") < 1000)
)
