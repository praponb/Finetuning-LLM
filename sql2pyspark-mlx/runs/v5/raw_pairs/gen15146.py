from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.max("cost").over(w2).alias("status_max_cost"),
    )
    .orderBy("id")
)
