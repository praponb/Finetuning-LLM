from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 86)
    .groupBy("status", F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(
        F.count("cost").alias("cost_count"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .filter(F.col("cost_count") >= 1)
    .orderBy("shipped_month")
)
