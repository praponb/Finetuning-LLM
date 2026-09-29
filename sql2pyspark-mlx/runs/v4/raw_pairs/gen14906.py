from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("id", "dept_name")
    .intersectAll(
        df_departments
        .filter(F.col("location") == "New York")
        .select("id", "dept_name")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget")).otherwise(0)).alias("finance_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .orderBy(F.col("dept_name").desc_nulls_first())
)
