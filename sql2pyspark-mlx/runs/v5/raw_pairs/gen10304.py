from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_shipments
    .filter(F.year("shipped_date") >= 2024)
    .select(
        "*",
        F.avg("cost").over(w).alias("carrier_avg_cost"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("YEAR", "delivered_date").alias("year_start"),
        F.when(F.col("carrier_avg_cost") >= 83, "PREMIUM").otherwise("STANDARD").alias("carrier_avg_cost_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
        F.countDistinct("status").alias("distinct_status"),
        F.stddev_samp("cost").alias("cost_stddev"),
    )
    .orderBy("carrier_avg_cost_tier")
)
