from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("end_date", "id")

base = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-11-13").cast("date"))
    .withColumn("status_plan_key", F.concat_ws(",", "status", "plan"))
    .withColumn("status_group", F.when(F.col("status").isin(["EXPIRED", "ACTIVE"]), "Core").otherwise("Other"))
)

enriched = (
    base
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.segment"), F.col("c.email"), F.col("c.signup_date"))
)

scored = (
    enriched
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

matched_keys = (
    scored
    .select("id", "status")
    .intersectAll(
        df_subscriptions
        .filter(F.col("end_date").between("2024-03-17", "2024-06-01"))
        .select("id", "status")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

stats = (
    verified
    .groupBy(F.date_trunc("WEEK", "signup_date").alias("week_start"))
    .agg(
        F.count_if(F.col("segment") == "Corporate").alias("corporate_events"),
        F.sum(F.when(F.col("plan") == "standard", F.col("cumulative_monthly_fee"))).alias("standard_total_cumulative_monthly_fee"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
    )
    .filter(F.col("basic_rows") > 1)
)

result = (
    stats
    .select("week_start", "monthly_fee_stddev", "basic_rows")
    .orderBy(F.col("basic_rows").asc_nulls_last())
)
