from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")
w2 = Window.partitionBy("order_id", "status")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "shipped_date",
        "delivered_date",
        "order_id",
        "cost",
        F.first("delivered_date").over(w).alias("first_delivered_date"),
        F.max("cost").over(w2).alias("order_id_max_cost"),
    )
)
