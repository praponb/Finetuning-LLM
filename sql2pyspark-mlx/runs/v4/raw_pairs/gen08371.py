from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy(F.col("paid_at").asc_nulls_first(), "id")

latest = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "latest_rank",
        F.when(F.col("status").rlike("^(SUCCESS|FAILED)$"), "PRIORITY").when(F.col("status").rlike("^(REFUNDED)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "order_id")
)
