from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_first())
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("status", "order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .select(
        "*",
        F.percent_rank().over(w).alias("cost_percent_rank"),
        F.rank().over(w2).alias("cost_rank"),
        F.last("cost", ignorenulls=True).over(w3).alias("last_known_cost"),
        F.ntile(3).over(w4).alias("cost_bucket"),
        F.last("carrier", ignorenulls=True).over(w5).alias("last_known_carrier"),
    )
)
