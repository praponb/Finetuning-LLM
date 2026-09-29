from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "device").orderBy(F.col("started_at").desc(), "id")

ranked = (
    df_sessions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "device", "started_at")
)

result = (
    ranked
    .select(
        "id",
        "device",
        "started_at",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "CORE").when(F.col("device").rlike("^(mobile)$"), "GROUP_B").otherwise("GROUP_A").alias("device_zone"),
    )
)
