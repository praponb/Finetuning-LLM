from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.orderBy(F.col("cost").desc(), "id")
w3 = Window.orderBy(F.col("cost").desc_nulls_last())

result = (
    df_shipments
    .select(
        "*",
        F.row_number().over(w_ordered).alias("event_seq"),
        F.ntile(4).over(w2).alias("cost_bucket"),
        F.rank().over(w3).alias("cost_rank"),
    )
)
