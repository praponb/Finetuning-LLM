from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")
w_part = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("status", "order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_last())
w_part_2 = Window.orderBy("cost", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_shipments
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(4).over(w_part).alias("cost_bucket"),
        F.last("carrier", ignorenulls=True).over(w3).alias("last_known_carrier"),
        F.dense_rank().over(w4).alias("cost_dense_rank"),
        F.avg("cost").over(w_part_2).alias("cost_avg_3"),
    )
)
