from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")

base = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-07-13").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.date_format("end_date", "yyyy-MM").alias("end_date_label"),
        F.round((F.col("monthly_fee") * 100), 2).alias("monthly_fee_scaled"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

daily = (
    deduped
    .withColumn("activity_month", F.date_trunc("MONTH", "start_date"))
    .groupingSets(
        [["customer_id", "activity_month"], ["customer_id"]],
        "customer_id",
        "activity_month",
    )
    .agg(
        F.max(F.when(F.col("plan") == "basic", F.col("monthly_fee"))).alias("basic_max_monthly_fee"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.grouping("customer_id").alias("grouping_id"),
    )
    .filter(F.col("enterprise_monthly_fee") >= 0)
)

result = (
    daily
    .select(
        "basic_max_monthly_fee",
        "enterprise_monthly_fee",
        "status_list",
        "grouping_id",
    )
)
