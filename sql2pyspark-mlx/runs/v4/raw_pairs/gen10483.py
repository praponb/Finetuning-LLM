from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_shipments
    .select(
        "*",
        F.first("cost", ignorenulls=True).over(w).alias("first_known_cost"),
        F.last("carrier", ignorenulls=True).over(w2).alias("last_known_carrier"),
    )
)

result = (
    scored
    .withColumn("cost_tier", F.when(F.col("cost") >= 91, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "delivered_date"))
    .groupingSets(
        [["cost_tier", "week_start"], ["cost_tier"]],
        "cost_tier",
        "week_start",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("num_status") >= 1)
)
