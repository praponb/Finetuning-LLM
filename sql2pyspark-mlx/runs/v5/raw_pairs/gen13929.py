from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").asc_nulls_first(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
    )
)

cleaned_subscriptions = (
    cleaned
    .filter(F.col("end_date") > F.lit("2023-05-26").cast("date"))
    .select(
        "*",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.date_format("end_date", "yyyy-MM-dd").alias("end_date_label"),
    )
)

valid = (
    cleaned_subscriptions
    .filter(F.col("end_date") > "2025-10-18")
    .select("*", F.initcap(F.trim("plan")).alias("plan_title_2"))
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .withColumn("week_start", F.date_trunc("WEEK", "start_date"))
    .groupingSets([["week_start", "plan"], ["week_start"]], "week_start", "plan")
    .agg(
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_rows"),
        F.round(F.avg("days_end_date_to_start_date"), 2).alias("avg_days_end_date_to_start_date"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
    )
)
