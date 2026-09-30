from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier")
w2 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")

metrics = (
    df_shipments
    .filter(F.col("order_id").isin([9, 14, 38]))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w2))
    .withColumn("status_max_cost", F.max("cost").over(w3))
)

result = (
    metrics
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("activity_month"), "status")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("first_known_cost"))).alias("returned_total_first_known_cost"),
        F.max("cost").alias("max_cost"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").desc())
)
