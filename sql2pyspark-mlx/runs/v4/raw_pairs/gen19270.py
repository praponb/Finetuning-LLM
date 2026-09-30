from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 7289)
    .groupBy(F.month("opened_date").alias("opened_month"), "region")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
        F.max("opened_date").alias("last_opened_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
    .orderBy("last_opened_date")
)
