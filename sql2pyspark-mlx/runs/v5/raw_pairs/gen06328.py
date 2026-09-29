from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("location").isin(["New York", "Chiang Mai"]), "group_a").otherwise("other").alias("location_group"),
    )
    .agg(
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.avg("budget").alias("avg_budget"),
        F.sum("budget").alias("total_budget"),
    )
    .orderBy("total_budget")
)
