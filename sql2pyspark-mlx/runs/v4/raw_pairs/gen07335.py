from pyspark.sql import functions as F

result = (
    df_departments
    .withColumn("budget_tier", F.when(F.col("budget") >= 3059000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["budget_tier", "location"], ["budget_tier"]],
        "budget_tier",
        "location",
    )
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
    )
    .orderBy(F.col("budget_tier").desc_nulls_first())
)
