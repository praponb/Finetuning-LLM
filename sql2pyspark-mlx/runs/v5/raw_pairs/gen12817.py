from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id")
w2 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

enriched = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        F.lag("cost").over(w).alias("prev_cost"),
        F.lead("cost").over(w).alias("next_cost"),
        F.row_number().over(w).alias("seq_no"),
        F.sum("cost").over(w2).alias("cumulative_cost"),
    )
)

result = (
    enriched
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "prev_cost",
        "next_cost",
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("max_shipment_cost").desc_nulls_last())
)
