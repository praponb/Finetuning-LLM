from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy(F.col("amount").desc())

filtered = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.concat_ws(",", "status", "method").alias("status_method_key"),
        F.coalesce("method", F.lit("DIRECT")).alias("resolved_method"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "resolved_method")
)
