from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 2272000)
    .cube(
        "location",
        F.when(F.col("budget") >= 3375000, "high").when(F.col("budget") >= 1975000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.sum(F.when(F.col("location") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
    .orderBy(F.col("chiang_mai_count").desc(), F.col("location").desc())
)
