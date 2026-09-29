from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc(), "id")

latest = (
    df_payments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select("id", "method", "amount", "status", "rank_in_group")
    .orderBy(F.col("id").asc_nulls_first())
)
