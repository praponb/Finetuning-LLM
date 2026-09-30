from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.count("rating").alias("rating_count"),
    )
    .orderBy("city_values", F.col("rating_count").desc())
)
