from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

enriched = (
    df_shipments
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w))
    .withColumn("cost_bucket", F.ntile(10).over(w2))
)

result = (
    enriched
    .rollup(F.date_trunc("MONTH", "delivered_date").alias("activity_month"), "status")
    .agg(
        F.round(F.sum("cost_bucket") / F.nullif(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)), F.lit(0)), 2).alias("cost_bucket_per_in_transit"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("cost_bucket_per_in_transit") < 0)
    .orderBy(F.col("total_cost").desc())
)
