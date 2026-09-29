from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").asc_nulls_last())
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w3 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_shipments
    .withColumn("cost_percent_rank", F.percent_rank().over(w))
    .withColumn("next_cost", F.lead("cost", 1, 0).over(w2))
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w3))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "cost_percent_rank",
        "first_known_carrier",
    )
    .orderBy("order_id", "status")
)
