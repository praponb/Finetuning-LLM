from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_last())
w4 = Window.partitionBy("order_id").orderBy(F.col("amount_share").desc_nulls_last(), "id")
w5 = Window.partitionBy("order_id").orderBy("shipping_fee", "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

enriched = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        F.sum("amount").over(w).alias("running_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.dense_rank().over(w3).alias("amount_dense_rank"),
    )
)

enriched_payments = (
    enriched
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.status") == "RETURNED")
    .select(
        "s.id",
        "s.order_id",
        "s.amount_share",
        F.col("b.discount"),
        F.col("b.shipping_fee"),
    )
)

enriched_payments_2 = (
    enriched_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.order_date"))
)

deduped = (
    enriched_payments_2
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped_payments = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped_payments
    .join(amount_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "discount", "rank_in_group", F.col("sum_payment_amount"))
)
