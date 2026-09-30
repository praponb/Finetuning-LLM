from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id")
w2 = Window.orderBy("cost", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.lag("cost").over(w).alias("prev_cost"),
        F.row_number().over(w).alias("seq_no"),
        F.ntile(10).over(w2).alias("cost_bucket"),
    )
)
