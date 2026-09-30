from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget_rounded").desc_nulls_last())
w_ordered = Window.partitionBy("location")
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget_rounded").asc_nulls_last())
w4 = Window.partitionBy("dept_name")

valid = (
    df_departments
    .filter(F.col("budget").between(1310000, 1488000))
    .withColumn("location_dept_name", F.concat("location", F.lit(" - "), "dept_name"))
    .withColumn("budget_rounded", F.round("budget", 2))
)

remaining_keys = (
    valid
    .select("dept_name", "id")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "HR")
        .select("dept_name", "id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.dense_rank().over(w).alias("budget_rounded_dense_rank"),
        F.stddev_samp("budget").over(w_ordered).alias("location_stddev_samp_budget"),
        F.cume_dist().over(w3).alias("budget_rounded_cume_dist"),
        F.sum("budget").over(w4).alias("dept_name_sum_budget"),
        F.min("budget_rounded").over(w_ordered).alias("location_min_budget_rounded"),
    )
)

result = (
    enriched
    .groupBy("location")
    .pivot("dept_name", ["Finance", "Sales", "Engineering"])
    .agg(F.max("location_min_budget_rounded").alias("amount"))
)
