from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "Berlin"]) & F.col("location").isin(["Bangkok", "New York"]) & F.col("location").isin(["Tokyo", "Bangkok", "London"]))
    .groupBy(
        "location",
        "dept_name",
        F.when(F.col("budget") >= 3743000, "high").when(F.col("budget") >= 1093000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.max("budget").alias("max_budget"),
        F.countDistinct("budget").alias("unique_locations"),
    )
    .filter(F.col("unique_locations") > 5)
)
