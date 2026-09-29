from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("plan_status", F.concat("plan", F.lit(" - "), "status"))
    .orderBy("plan", "status")
)
