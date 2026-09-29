from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")
w2 = Window.partitionBy("carrier", "order_id").orderBy("shipped_date", "id").rowsBetween(-3, -1)

enriched = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "status",
        F.ntile(3).over(w).alias("cost_bucket"),
        F.avg("cost").over(w2).alias("cost_avg_3_prior"),
    )
)

result = (
    enriched
    .withColumn("quarter_start", F.date_trunc("QUARTER", "shipped_date"))
    .withColumn("year_start", F.date_trunc("YEAR", "shipped_date"))
    .withColumn("cost_bucket_tier", F.when(F.col("cost_bucket") >= 9, "HIGH").otherwise("LOW"))
    .rollup("quarter_start", "year_start", "cost_bucket_tier")
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost_avg_3_prior"))).alias("delivered_total_cost_avg_3_prior"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("cost_bucket") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("cost_bucket_per_delivered"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("cost_bucket_per_delivered") < 1000000)
)
