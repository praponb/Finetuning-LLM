from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city", "country")
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.count("rating").alias("rating_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .filter(F.col("unique_citys") > 3)
)
