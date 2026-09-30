from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "HR", "Engineering"]) & F.col("dept_name").isNotNull())
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Finance", "Marketing"]), "Group A").otherwise("Other"))
)

agg = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3919000, "high").when(F.col("budget") >= 914000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"))
)

result = (
    agg
    .select("budget_bucket", "dept_name", "dept_name_list")
    .orderBy("budget_bucket")
)
