from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee_scaled")

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("end_date") < "2025-01-06"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.round((F.col("monthly_fee") * 0.85), 2).alias("monthly_fee_scaled"),
        F.trim("plan").alias("clean_plan"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)
