from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w2 = Window.partitionBy("status", "carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_shipments
    .filter(F.col("shipped_date") >= "2024-01-01")
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.lag("cost").over(w_part).alias("prev_cost"),
        F.row_number().over(w_part).alias("seq_no"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
    )
)

result = (
    with_windows
    .select("id", "carrier", "delivered_date", "status", "prev_cost", "seq_no")
    .orderBy(F.col("delivered_date").asc_nulls_last(), F.col("status").desc())
)
