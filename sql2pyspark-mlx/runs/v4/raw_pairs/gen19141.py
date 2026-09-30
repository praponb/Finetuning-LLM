from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")

base = (
    df_payments
    .filter(F.col("method").isNotNull() & F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .withColumn("method_group", F.when(F.col("method").isin(["paypal", "bank_transfer"]), "Priority").otherwise("Other"))
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
