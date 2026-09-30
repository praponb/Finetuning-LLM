from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("location") == "Berlin") & (F.col("location") == "Tokyo"))
    .withColumn("location_prefix", F.substring("location", 1, 2))
)

departments_stats = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3967000, "high").when(F.col("budget") >= 774000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)

result = departments_stats.filter(F.col("pct_tokyo") < 50)
