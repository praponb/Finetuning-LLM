from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "London")
    .groupBy("country")
    .agg(
        F.max("rating").alias("max_rating"),
        F.avg("rating").alias("avg_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy("country")
)
