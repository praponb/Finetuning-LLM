from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Tokyo", "Berlin"]))
    .groupBy(
        F.when(F.col("budget") >= 3875000, "high").when(F.col("budget") >= 1257000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.count("budget").alias("budget_count"),
    )
    .filter(F.col("budget_count") >= 5)
    .drop("budget_count")
    .orderBy("budget_bucket")
    .limit(5)
)
