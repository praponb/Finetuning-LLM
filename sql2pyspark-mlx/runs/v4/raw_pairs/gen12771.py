from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

metrics = (
    df_shipments
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.cume_dist().over(w2).alias("cost_cume_dist"),
        F.last("carrier", ignorenulls=True).over(w3).alias("last_known_carrier"),
    )
)

result = (
    metrics
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "seq_no",
        "last_known_carrier",
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("min_shipment_cost").desc())
)
