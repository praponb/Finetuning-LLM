from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("signup_date")

filtered = (
    df_subscriptions
    .filter((F.col("status") == "PAUSED") & (F.col("end_date") >= F.to_date(F.lit("2023-09-12"))))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.city"), F.col("d.email"), F.col("d.signup_date"))
)

attributed = (
    joined
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.end_date") >= F.col("sub.start_date")) & (F.col("a.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

ranked = (
    attributed
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "id",
        "plan",
        "monthly_fee",
        "email",
        "subscription_id",
        "subscription_plan",
        "subscription_monthly_fee",
        "row_num",
        F.when(F.col("plan").rlike("^(enterprise|premium)$"), "GROUP_B").when(F.col("plan").rlike("^(basic|standard)$"), "PRIORITY").otherwise("EXTENDED").alias("plan_zone"),
    )
)
