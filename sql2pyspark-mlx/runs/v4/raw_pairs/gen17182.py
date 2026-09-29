from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc(), "id")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

with_windows = (
    df_accounts
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

result = (
    with_windows
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "status",
        "event_seq",
        "seq_no",
        F.col("sum_order_discount"),
    )
)
