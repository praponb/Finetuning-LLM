from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") >= "2025-03-01") | F.col("start_date").between("2023-05-01", "2025-02-01"))
    .withColumn("end_date_month_end", F.last_day("end_date"))
    .withColumn("end_date_plus_18d", F.date_add("end_date", 18))
    .orderBy("start_date", "end_date_plus_18d")
)
