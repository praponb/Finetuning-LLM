from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id")
w2 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("cost") > 79)
    .select("*", F.length("status").alias("status_length"))
)

result = (
    cleaned
    .select(
        "*",
        F.lag("cost", 1, 0).over(w).alias("prev_cost"),
        F.sum("status_length").over(w2).alias("running_status_length"),
    )
)
