from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("rating") > 3.5)
    .select("*", F.upper("city").alias("city_upper"))
)

result = (
    base
    .groupBy(
        F.when(F.col("rating") >= 3.9, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("rating_per_us"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
