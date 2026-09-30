from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy(F.col("monthly_fee").desc(), "id")

ranked = (
    df_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "plan", "start_date", "end_date", "status")
)

result = (
    ranked
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        F.when(F.col("status").rlike("^(EXPIRED|CANCELLED)$"), "EXTENDED").when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
    )
)
