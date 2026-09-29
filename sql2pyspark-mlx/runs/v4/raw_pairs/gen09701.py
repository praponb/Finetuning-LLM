from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("monthly_fee") >= 160) & (F.col("end_date") < "2024-12-01"))
)

summary = (
    base
    .groupBy(
        "customer_id",
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(F.sort_array(F.collect_set("plan")).alias("plan_list"))
)

result = (
    summary
    .select("customer_id", "end_cohort", "plan_list")
    .orderBy("end_cohort")
)
