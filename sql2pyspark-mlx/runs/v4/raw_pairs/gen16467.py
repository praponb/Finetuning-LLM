from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .groupBy(
        "country",
        F.when(F.col("city").isin(["London", "Chiang Mai"]), "priority").otherwise("other").alias("city_group"),
    )
    .agg(
        F.max("rating").alias("max_rating"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy(F.col("max_rating").desc(), "city_group", "country")
    .limit(20)
)
