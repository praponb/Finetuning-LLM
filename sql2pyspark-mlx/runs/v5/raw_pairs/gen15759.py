from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.avg("rating").alias("avg_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy("city")
)
