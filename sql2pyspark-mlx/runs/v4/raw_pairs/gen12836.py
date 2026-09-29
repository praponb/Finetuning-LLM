from pyspark.sql import functions as F

max_start_date = df_subscriptions.filter(F.col("monthly_fee").between(140, 200)).agg(F.max("start_date")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("start_date") >= max_start_date)
    .select("id", "start_date", "plan")
    .orderBy(F.col("start_date").desc())
)
