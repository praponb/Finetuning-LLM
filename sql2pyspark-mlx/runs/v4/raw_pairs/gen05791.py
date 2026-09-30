from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")

ranked = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "latest_rank",
        F.when(F.col("method").rlike("^(cash|promptpay)$"), "OTHER_ZONE").when(F.col("method").rlike("^(card|paypal)$"), "CORE").otherwise("PRIORITY").alias("method_zone"),
    )
)
