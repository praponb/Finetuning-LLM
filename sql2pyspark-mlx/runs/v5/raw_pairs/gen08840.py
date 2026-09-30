from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_3"),
        F.lag("cost", 1, 0).over(w2).alias("prev_cost"),
    )
)
