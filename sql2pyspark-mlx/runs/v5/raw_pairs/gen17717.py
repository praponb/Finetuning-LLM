from pyspark.sql import functions as F

matched_keys = (
    df_departments
    .select("dept_name", "location")
    .exceptAll(
        df_departments
        .filter(F.col("location") != "New York")
        .select("dept_name", "location")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("budget_per_engineering"),
    )
    .orderBy(F.col("budget_per_engineering").asc_nulls_last())
)
