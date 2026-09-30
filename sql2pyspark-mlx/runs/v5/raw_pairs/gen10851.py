from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Siam%"))
    .groupBy("country")
    .agg(
        F.count("rating").alias("rating_count"),
        F.max("rating").alias("max_rating"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)
