from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("order_id")

result = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "UPS", "Flash"]))
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        "order_id",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_7"),
        F.count("*").over(w2).alias("order_id_count"),
    )
)
