from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .withColumn("clean_country", F.trim("country"))
)

suppliers_stats = (
    base
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.avg("rating").alias("avg_rating"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("avg_rating") > 2.2)
    .select("country", "city_list", "avg_rating")
    .orderBy("country")
    .limit(50)
)
