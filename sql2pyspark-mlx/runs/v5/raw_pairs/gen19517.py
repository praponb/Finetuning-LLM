from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-08-07 21:00:00"))) & (~F.col("method").eqNullSafe(F.lit("bank_transfer"))))
    .withColumn("is_bank_transfer", F.when(F.col("method") == "bank_transfer", 1).otherwise(0))
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "amount", "status")
)
