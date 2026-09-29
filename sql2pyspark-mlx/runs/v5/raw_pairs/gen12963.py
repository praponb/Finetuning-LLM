from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

with_windows = (
    df_shipments
    .withColumn("cost_bucket", F.ntile(4).over(w))
    .withColumn("carrier_sum_cost", F.sum("cost").over(w2))
)

result = (
    with_windows
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "status",
        "carrier_sum_cost",
        F.col("sum_order_item_quantity"),
    )
    .orderBy("id")
    .limit(20)
)
