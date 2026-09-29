from pyspark.sql import functions as F

src = df_subscriptions.filter(F.col("plan").isin(["enterprise", "basic"]))

summary = (
    src
    .groupBy("plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_expired"),
        F.max("end_date").alias("latest_end_date"),
    )
)

result = summary.orderBy("latest_end_date", F.col("plan").desc())
