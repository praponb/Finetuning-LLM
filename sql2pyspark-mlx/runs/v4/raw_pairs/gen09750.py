from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("end_date") > F.to_date(F.lit("2024-05-31")))
    .withColumn("status_plan_key", F.concat_ws(",", "status", "plan"))
)

result = (
    valid
    .select("plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("end_date") <= "2023-07-30")
        .select("plan")
    )
)
