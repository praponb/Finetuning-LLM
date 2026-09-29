from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").asc_nulls_first(), "id")

deduped = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "latest_rank",
        F.when(F.col("status").rlike("^(CANCELLED|EXPIRED)$"), "PRIORITY").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy("latest_rank")
)
