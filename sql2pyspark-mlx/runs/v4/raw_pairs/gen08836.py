from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "DHL"]))
    .select(
        "id",
        "status",
        "shipped_date",
        "cost",
        "order_id",
        "delivered_date",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_4"),
        F.lead("cost", 2).over(w2).alias("next_cost"),
    )
)
