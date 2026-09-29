from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "JP")
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("country") == "DE", F.col("rating")).otherwise(0)).alias("de_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .orderBy(F.col("de_rating").desc())
)
