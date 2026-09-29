from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "status",
        "carrier",
        "delivered_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_4"),
    )
    .orderBy("carrier")
)
