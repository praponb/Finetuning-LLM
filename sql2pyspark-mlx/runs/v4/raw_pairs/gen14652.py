from pyspark.sql import functions as F

eligible = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "left_semi")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("sqft") >= 17893, "HIGH").otherwise("LOW").alias("sqft_tier"),
        "city",
    )
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
    )
    .orderBy("sqft_tier")
)
