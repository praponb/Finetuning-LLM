from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_last(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

deduped = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "row_num",
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("delivered_date").desc_nulls_first())
)
