from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost_whole", "id")
w2 = Window.orderBy("cost_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.ceil("cost").alias("cost_whole"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.ntile(5).over(w).alias("cost_whole_bucket"),
        F.last("cost_whole", ignorenulls=True).over(w2).alias("last_known_cost_whole"),
    )
)
