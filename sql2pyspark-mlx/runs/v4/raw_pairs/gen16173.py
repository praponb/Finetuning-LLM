from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() | F.month("end_date").isin([6, 10, 11]))
    .select(
        "end_date",
        "start_date",
        F.when(F.col("plan").isin(["premium", "basic"]), "Priority").otherwise("Other").alias("plan_group"),
    )
    .orderBy("end_date")
)
