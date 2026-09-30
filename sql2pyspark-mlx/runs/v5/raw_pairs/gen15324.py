from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy("monthly_fee", "id")

latest = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        "latest_rank",
        F.when(F.col("status").rlike("^(CANCELLED|ACTIVE)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("status").desc())
)
