from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").asc_nulls_first(), "id")

deduped = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select("id", "method", "amount", "paid_at", "dedup_rank")
    .orderBy(F.col("dedup_rank").desc_nulls_first())
)
