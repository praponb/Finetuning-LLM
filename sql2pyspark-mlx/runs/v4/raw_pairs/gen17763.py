from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .select(
        "*",
        F.lower("location").alias("location_lower"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("dept_name_clean"),
    )
)

result = (
    filtered
    .withColumn("budget_tier", F.when(F.col("budget") >= 4383000, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["budget_tier", "dept_name"], ["budget_tier"], []],
        "budget_tier",
        "dept_name",
    )
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.countDistinct("location").alias("distinct_locations"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("dept_name").desc_nulls_first())
)
