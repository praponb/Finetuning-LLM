from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", F.month("opened_date").alias("opened_month"))
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("total_sqft") < 70000)
    .select("region", "opened_month", "city_list", "total_sqft")
    .orderBy(F.col("total_sqft").desc(), "region", "opened_month")
    .limit(50)
)
