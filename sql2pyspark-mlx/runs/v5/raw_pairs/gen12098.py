from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")

deduped = (
    df_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "dedup_rank",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
