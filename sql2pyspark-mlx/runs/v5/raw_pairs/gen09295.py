from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at")

latest = (
    df_payments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "method", "status")
)

result = (
    latest
    .select(
        "id",
        "status",
        F.when(F.col("status").rlike("^(REFUNDED|FAILED)$"), "GROUP_A").when(F.col("status").rlike("^(SUCCESS)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy("status_zone", F.col("id").desc())
)
