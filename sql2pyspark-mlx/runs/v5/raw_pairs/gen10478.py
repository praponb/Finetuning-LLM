from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w_hist = Window.orderBy(F.col("monthly_fee").asc_nulls_last())

filtered = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-08-01", "2025-06-01"))
    .withColumn("days_start_date_to_end_date", F.datediff("end_date", "start_date"))
    .withColumn("end_date_label", F.date_format("end_date", "yyyy-MM"))
)

remaining_keys = (
    filtered
    .select("plan", "id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("start_date").between("2024-05-01", "2025-09-01"))
        .select("plan", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered_subscriptions = (
    verified
    .filter(F.col("days_start_date_to_end_date") >= 33)
    .select(
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "days_start_date_to_end_date",
        "end_date_label",
        F.length("plan").alias("plan_length"),
    )
)

valid = (
    filtered_subscriptions
    .filter(F.col("monthly_fee") > 120)
    .select(
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "days_start_date_to_end_date",
        "end_date_label",
        "plan_length",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
)

scored = (
    valid
    .filter(F.col("plan") == "basic")
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.dense_rank().over(w_hist).alias("monthly_fee_dense_rank"),
    )
)

result = (
    scored
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .groupingSets(
        [["country", "plan"], ["country"]],
        F.col("d.country").alias("country"),
        F.col("s.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("s.monthly_fee_share")).alias("avg_monthly_fee_share"),
        F.max(F.col("s.plan_length")).alias("max_plan_length"),
        F.grouping_id().alias("grouping_level"),
    )
)
