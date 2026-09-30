from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-07-01", "2025-07-01"))
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_3"),
    )
    .orderBy(F.col("cost_moving_avg_3").desc())
)
