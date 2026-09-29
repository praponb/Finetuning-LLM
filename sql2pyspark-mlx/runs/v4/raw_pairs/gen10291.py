from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") != "basic") & (F.col("status") != "PAUSED"))
    .select("id", "end_date", F.dayofmonth("end_date").alias("end_date_day"))
    .orderBy(F.col("end_date").desc())
)
