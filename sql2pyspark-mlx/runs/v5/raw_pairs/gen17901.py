from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-08-18", "2023-10-01") & F.col("plan").isin(["enterprise", "premium"]))
    .select("start_date", "plan", "status", F.upper("plan").alias("plan_upper"))
)
