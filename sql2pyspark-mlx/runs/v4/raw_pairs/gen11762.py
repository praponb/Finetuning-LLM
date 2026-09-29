from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").asc_nulls_first(), "id")

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
        "plan",
        "start_date",
        "end_date",
        F.when(F.col("plan").rlike("^(enterprise|standard)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
    .orderBy(F.col("plan").desc_nulls_last())
)
