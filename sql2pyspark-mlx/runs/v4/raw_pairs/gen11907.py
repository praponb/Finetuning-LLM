from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("delivered_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("carrier") == "Flash")
    .select(
        "id",
        "carrier",
        "order_id",
        "delivered_date",
        "status",
        "shipped_date",
        F.row_number().over(w).alias("rn"),
        F.ntile(10).over(w2).alias("bucket"),
    )
)
