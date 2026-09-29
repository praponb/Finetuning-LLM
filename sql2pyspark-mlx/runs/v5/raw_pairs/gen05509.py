from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w3 = Window.partitionBy("status").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        F.sum("cost").over(w).alias("cumulative_cost"),
        F.row_number().over(w2).alias("event_seq"),
        F.percent_rank().over(w3).alias("cost_percent_rank"),
    )
)
