from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "monthly_fee", F.upper("plan").alias("plan_upper"))
    .orderBy(F.col("id").desc(), F.col("plan_upper").desc())
)
