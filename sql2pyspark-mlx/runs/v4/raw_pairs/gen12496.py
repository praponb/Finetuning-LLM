from pyspark.sql import functions as F

matched_keys = (
    df_departments
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("budget") < 3483000)
        .select("id")
    )
)

kept_rows = (
    df_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("budget_tier", F.when(F.col("budget") >= 3327000, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["budget_tier", "location"], ["budget_tier"], []],
        "budget_tier",
        "location",
    )
    .agg(
        F.max(F.when(F.col("dept_name") == "Legal", F.col("budget"))).alias("legal_max_budget"),
        F.count_if(F.col("location") == "New York").alias("new_york_events"),
        F.count_if(F.col("dept_name") == "Engineering").alias("engineering_events"),
        F.grouping("budget_tier").alias("grouping_id"),
    )
    .filter(F.col("legal_max_budget") < 0)
)
