from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum("sqft").alias("total_sqft"),
    )
    .orderBy("opened_month")
)
