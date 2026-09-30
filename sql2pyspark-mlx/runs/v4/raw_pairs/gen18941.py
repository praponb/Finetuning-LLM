from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")
w2 = Window.partitionBy("status")

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "cost",
        "status",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.min("cost").over(w2).alias("status_min_cost"),
    )
)
