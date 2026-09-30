from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "delivered_date",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_7"),
        F.percent_rank().over(w2).alias("delivered_date_pct_rank"),
    )
)
