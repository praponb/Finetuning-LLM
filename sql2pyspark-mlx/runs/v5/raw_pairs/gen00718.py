from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
    )
    .filter(F.col("max_monthly_fee") < 190)
    .select("end_cohort", "max_monthly_fee", "basic_monthly_fee")
    .orderBy(F.col("max_monthly_fee").desc(), "end_cohort")
    .limit(5)
)
