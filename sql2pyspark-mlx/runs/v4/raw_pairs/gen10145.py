from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc_nulls_last(), "id")

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("method_status_key", F.concat_ws("-", "method", "status"))
    .withColumn("method_filled", F.coalesce("method", F.lit("DIRECT")))
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
