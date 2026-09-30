from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-05-19").cast("date"))
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.concat("plan", F.lit("/"), "status").alias("plan_status"),
    )
)

result = (
    valid
    .groupBy("status")
    .pivot("plan", ["premium", "standard"])
    .agg(F.max("monthly_fee").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
