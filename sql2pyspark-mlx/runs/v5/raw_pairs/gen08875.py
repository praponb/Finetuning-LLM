from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc())
amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

ranked = (
    df_order_items
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "rank_in_group",
        F.col("avg_payment_amount"),
    )
)
