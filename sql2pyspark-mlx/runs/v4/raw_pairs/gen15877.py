from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id", "carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .select(
        "*",
        F.first("cost", ignorenulls=True).over(w_ordered).alias("first_known_cost"),
        F.last("carrier", ignorenulls=True).over(w2).alias("last_known_carrier"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
