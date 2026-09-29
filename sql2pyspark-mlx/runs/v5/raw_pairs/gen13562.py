from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id")
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "status",
        "delivered_date",
        "carrier",
        "cost",
        F.ntile(2).over(w).alias("delivered_date_ntile"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
    )
)
