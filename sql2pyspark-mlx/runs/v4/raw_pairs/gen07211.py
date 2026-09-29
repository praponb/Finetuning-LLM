from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.year("end_date") >= 2024) & (F.col("status") == "EXPIRED"))
    .withColumn("days_start_date_to_end_date", F.datediff("end_date", "start_date"))
    .withColumn("plan_group", F.when(F.col("plan").isin(["premium", "basic"]), "Priority").otherwise("Other"))
    .orderBy("plan", "plan_group")
)
