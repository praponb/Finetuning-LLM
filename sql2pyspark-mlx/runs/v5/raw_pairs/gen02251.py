from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)
