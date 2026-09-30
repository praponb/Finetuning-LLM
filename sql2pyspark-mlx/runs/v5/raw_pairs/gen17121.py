from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("avg_rating") >= 30)
    .select("country", "avg_rating", "city_list")
    .orderBy("country")
)
