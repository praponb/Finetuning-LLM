from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

with_windows = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.row_number().over(w).alias("event_seq"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
    )
)

result = (
    with_windows
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "unit_price",
        "event_seq",
        "next_quantity",
        F.col("sum_payment_amount"),
    )
)
