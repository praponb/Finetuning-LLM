from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("status").orderBy("start_date", "id")
w3 = Window.partitionBy("plan").orderBy("start_date", "id")
w4 = Window.partitionBy("city").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("next_monthly_fee", "id")
w_grp = Window.partitionBy("city").orderBy("monthly_fee_bucket", "id")
w8 = Window.partitionBy("city").orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.orderBy("cumulative_monthly_fee", "id")

filtered = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-08-19").cast("date"))
    .select("*", F.initcap(F.trim("plan")).alias("plan_title"))
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"))
)

enriched_subscriptions = (
    enriched
    .filter(F.col("monthly_fee") >= 210)
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "city",
        F.ntile(5).over(w).alias("monthly_fee_bucket"),
        F.lag("monthly_fee", 1).over(w2).alias("prev_monthly_fee"),
        F.lead("monthly_fee", 2).over(w3).alias("next_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w4).alias("last_known_monthly_fee"),
        F.sum("monthly_fee").over(w5).alias("cumulative_monthly_fee"),
    )
)

metrics = (
    enriched_subscriptions
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("next_monthly_fee_2", F.lead("monthly_fee", 1).over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("monthly_fee_avg_3", F.avg("monthly_fee").over(w8))
    .withColumn("next_monthly_fee_3", F.lead("monthly_fee", 2).over(w9))
)

result = (
    metrics
    .groupBy(F.date_trunc("QUARTER", "start_date").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("num_cities") > 1)
)
