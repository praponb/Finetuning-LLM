from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc_nulls_last())
w2 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
    )
)
