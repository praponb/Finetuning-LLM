from pyspark.sql import functions as F

result = (
    df_suppliers
    .rollup("country", "city")
    .agg(
        F.count("rating").alias("rating_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy(F.col("country_values").desc())
)
