from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "status",
        "shipped_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_7"),
    )
    .orderBy("shipped_date", F.col("cost_moving_avg_7").desc())
)
