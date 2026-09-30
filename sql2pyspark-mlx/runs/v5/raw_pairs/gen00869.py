from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
    )
    .orderBy(F.col("pct_london").desc())
)
