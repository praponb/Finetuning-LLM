from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.month("end_date").isin([7, 8, 9]))
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

summary = (
    src
    .groupBy(
        F.when(F.col("monthly_fee") >= 186, "high").when(F.col("monthly_fee") >= 106, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.max("end_date").alias("last_end_date"))
)

result = summary.orderBy(F.col("monthly_fee_bucket").desc())
