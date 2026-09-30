from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w2 = Window.partitionBy("order_id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.min("cost").over(w2).alias("order_id_min_cost"),
    )
)
