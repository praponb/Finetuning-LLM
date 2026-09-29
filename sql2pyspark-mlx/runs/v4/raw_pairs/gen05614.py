from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("line_total").desc_nulls_last(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

cleaned = (
    df_order_items
    .filter(F.col("order_id") == 28)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

joined = (
    latest
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.status"), F.col("o.order_date"))
)

result = (
    joined
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "least_quantity_unit_price",
        "status",
        "order_date",
        F.col("sum_shipment_cost"),
    )
    .orderBy("status")
)
