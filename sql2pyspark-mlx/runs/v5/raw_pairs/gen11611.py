from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("status", "order_id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.sum("cost").over(w).alias("cumulative_cost"),
        F.rank().over(w2).alias("cost_rank"),
        F.max("cost").over(w3).alias("status_max_cost"),
    )
)
