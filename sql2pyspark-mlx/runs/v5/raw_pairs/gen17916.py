from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

ranked = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy("customer_id")
    .agg(
        F.countDistinct("device").alias("distinct_devices"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.min("started_at").alias("first_started_at"),
        F.min("started_at").alias("first_seen"),
    )
    .orderBy(F.col("customer_id").desc())
)
