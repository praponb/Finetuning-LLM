from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id")
w2 = Window.partitionBy("status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "cost",
        "status",
        "delivered_date",
        F.sum("cost").over(w).alias("running_cost"),
        F.sum("cost").over(w2).alias("running_cost_2"),
    )
)
