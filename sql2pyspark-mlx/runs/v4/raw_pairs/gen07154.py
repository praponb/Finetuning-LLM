from pyspark.sql import functions as F

max_start_date = df_subscriptions.filter(F.month("end_date").isin([3, 4, 9])).agg(F.max("start_date")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("start_date") > max_start_date)
    .select("id", "start_date", "status", "customer_id")
    .orderBy(F.col("id").desc(), F.col("start_date").desc())
)
