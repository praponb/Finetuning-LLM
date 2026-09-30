from pyspark.sql import functions as F

result = (
    df_shipments
    .rollup(F.date_trunc("WEEK", "shipped_date").alias("week_start"), "carrier")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_rows"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .filter(F.col("lost_rows") > 1)
    .orderBy(F.col("avg_cost").asc_nulls_last())
)
