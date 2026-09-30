from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        (F.col("pos") + 1).alias("item_no"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

result = (
    lines
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "item_no", "qty", F.col("sum_order_amount"))
    .orderBy(F.col("id").desc_nulls_last())
)
