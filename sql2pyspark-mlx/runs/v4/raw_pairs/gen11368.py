from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 3391000) | F.col("budget").between(1258000, 3893000))
    .groupBy(
        "location",
        F.when(F.col("dept_name").isin(["Engineering", "Marketing"]), "group_a").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy("location", "dept_name_group")
    .limit(50)
)
