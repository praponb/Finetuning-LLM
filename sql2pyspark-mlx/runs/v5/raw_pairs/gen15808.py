from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id", "carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2025-02-01")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.count("cost").over(w_hist).alias("running_count_cost"),
        F.sum("cost").over(w_hist).alias("running_sum_cost"),
        F.max("cost").over(w_hist).alias("running_max_cost"),
    )
)
