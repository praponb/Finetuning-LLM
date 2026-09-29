from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

ranked = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "shipping_fee",
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first())
)
