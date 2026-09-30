from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2025-04-01")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        "delivered_date",
        F.sum("cost").over(w).alias("running_cost"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
    )
)
