from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~((F.col("monthly_fee") >= 200) & F.month("start_date").isin([1, 4, 7])))
    .withColumn("plan_status", F.concat("plan", F.lit(" "), "status"))
    .orderBy("end_date")
)
