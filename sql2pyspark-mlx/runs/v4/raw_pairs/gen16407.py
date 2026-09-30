from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("delivered_date").desc(), "id")
w2 = Window.orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("order_id", "status").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.avg("cost").over(w2).alias("cost_avg_5"),
        F.row_number().over(w_recent).alias("seq_no_2"),
    )
)
