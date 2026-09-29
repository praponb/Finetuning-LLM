from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("order_id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

enriched = (
    df_order_items
    .filter(F.col("unit_price").between(180, 220))
    .select(
        "*",
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 4).alias("quantity_share"),
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4).alias("quantity_share_2"),
    )
)

result = (
    enriched
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity_share",
        F.col("min_payment_amount"),
    )
    .orderBy(F.col("unit_price").asc_nulls_first())
)
