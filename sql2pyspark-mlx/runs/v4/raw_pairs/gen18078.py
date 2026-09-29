from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc(), "id")
w2 = Window.partitionBy("carrier", "status").orderBy("cost", "id")

result = (
    df_shipments
    .filter(F.col("order_id") == 27)
    .select(
        "id",
        "order_id",
        "shipped_date",
        "carrier",
        "status",
        "cost",
        "delivered_date",
        F.row_number().over(w).alias("shipped_date_row"),
        F.first("delivered_date").over(w2).alias("first_delivered_date"),
    )
)
