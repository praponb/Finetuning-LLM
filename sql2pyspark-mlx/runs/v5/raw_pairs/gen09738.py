from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy(F.col("delivered_date").desc_nulls_first(), "id")

latest = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .withColumn("cost_tier", F.when(F.col("cost") >= 102, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["cost_tier", "status"], ["cost_tier"]], "cost_tier", "status")
    .agg(
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
        F.count_if(F.col("status") == "LOST").alias("lost_events"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("lost_cost") > 0)
    .orderBy(F.col("status").desc())
)
