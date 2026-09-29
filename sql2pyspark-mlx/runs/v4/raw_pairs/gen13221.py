from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.month("opened_date").alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") >= 9)
    .select("city", "opened_month", "region_values", "sqft_count")
    .orderBy("sqft_count")
)
