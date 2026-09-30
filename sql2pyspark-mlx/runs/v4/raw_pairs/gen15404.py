from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("carrier")
w3 = Window.partitionBy("status", "carrier").orderBy("days_since_shipped_date", "id")
w4 = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w5 = Window.partitionBy("status")

filtered = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .withColumn("days_since_shipped_date", F.datediff(F.current_date(), "shipped_date"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("DIRECT")))
)

result = (
    filtered
    .withColumn("cost_avg_2", F.avg("cost").over(w))
    .withColumn("carrier_sum_cost", F.sum("cost").over(w2))
    .withColumn("next_cost", F.lead("cost", 1).over(w3))
    .withColumn("prev_cost", F.lag("cost", 2).over(w4))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w5)), 4))
)
