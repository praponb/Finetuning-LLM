from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status")
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("order_id") == 23)
    .select(
        "id",
        "carrier",
        "status",
        "cost",
        "shipped_date",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
        F.round(F.avg("cost").over(w2), 2).alias("cost_moving_avg_3"),
    )
)
