from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~(F.col("region").isin(["Central", "West", "East"]) & (F.col("sqft") <= 15429) & F.col("store_name").like("%Mall%")))
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
)
