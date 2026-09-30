from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.orderBy("shipped_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("delivered_date", "id")

filtered = (
    df_shipments
    .filter(F.col("status") != "IN_TRANSIT")
    .select(
        "*",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
)

result = (
    filtered
    .filter(F.col("days_since_shipped_date").between(54, 80))
    .select(
        "id",
        "carrier",
        "cost",
        "status",
        "days_since_shipped_date",
        F.ntile(10).over(w).alias("cost_bucket"),
        F.avg("cost").over(w2).alias("cost_avg_3"),
        F.first("days_since_shipped_date", ignorenulls=True).over(w3).alias("first_known_days_since_shipped_date"),
        F.lead("cost", 1).over(w4).alias("next_cost"),
    )
)
