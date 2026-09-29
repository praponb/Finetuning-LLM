from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

scored = (
    df_order_items
    .filter(F.col("quantity") == 3)
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w))
    .withColumn("order_id_count_unit_price", F.count("unit_price").over(w2))
)

result = (
    scored
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "order_id_count_unit_price",
        F.col("avg_shipment_cost"),
    )
    .orderBy(F.col("order_id_count_unit_price").desc(), "avg_shipment_cost")
)
