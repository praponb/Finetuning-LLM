from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "status",
        F.count("cost").over(w).alias("order_id_count_cost"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
)
