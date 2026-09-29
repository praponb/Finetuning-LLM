from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.8, 3.6) | ~F.col("supplier_name").like("%Parts%"))
    .groupBy(
        F.when(F.col("city").isin(["London", "Bangkok"]), "core").otherwise("other").alias("city_group"),
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("min_rating") > 2.3)
)
