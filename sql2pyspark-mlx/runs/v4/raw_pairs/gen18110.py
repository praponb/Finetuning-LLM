from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "SG")
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
)
