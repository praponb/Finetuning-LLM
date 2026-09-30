from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("is_success", "id")

valid = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
        F.round("amount", 1).alias("amount_rounded"),
    )
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "amount", "status", "is_success", "amount_rounded")
)
