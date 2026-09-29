from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("order_id")

scored = (
    df_payments
    .select(
        "*",
        F.avg("amount").over(w_ordered).alias("amount_avg_6_prior"),
        F.sum("amount").over(w2).alias("order_id_sum_amount"),
    )
)

result = (
    scored
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "amount_avg_6_prior",
        "order_id_sum_amount",
        F.when(F.col("method").rlike("^(promptpay|paypal)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("method_zone"),
    )
    .orderBy(F.col("order_id_sum_amount").asc_nulls_first())
)
