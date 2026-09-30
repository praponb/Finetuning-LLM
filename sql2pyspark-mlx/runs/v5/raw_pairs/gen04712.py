from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") >= 2025)
    .withColumn("end_date_label", F.date_format("end_date", "yyyy-MM-dd"))
    .withColumn("status_plan_key", F.concat_ws("-", "status", "plan"))
)
