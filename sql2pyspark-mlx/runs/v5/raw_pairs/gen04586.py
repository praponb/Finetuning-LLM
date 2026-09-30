from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("job_title", "manager_id")
    .exceptAll(
        df_employees
        .filter(F.col("level").isin(["L2", "L1", "L5"]))
        .select("job_title", "manager_id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("salary_tier", F.when(F.col("salary") >= 160000, "HIGH").otherwise("LOW"))
    .rollup("salary_tier", "manager_id")
    .agg(
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_rows"),
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
        F.count_if(F.col("level") == "L1").alias("l1_events"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("manager_id")
)
