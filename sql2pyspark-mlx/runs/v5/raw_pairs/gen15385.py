from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("dept_name", "location")
w3 = Window.orderBy(F.col("budget").desc())

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_tier", F.when(F.col("budget") >= 4254000, "High").when(F.col("budget") >= 1377000, "Medium").otherwise("Low"))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("N/A")))
)

remaining_keys = (
    base
    .select("dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["HR", "Marketing", "Legal"]))
        .select("dept_name", "location")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w_grp).alias("budget_dense_rank"),
        F.count("budget").over(w2).alias("dept_name_count_budget"),
        F.dense_rank().over(w3).alias("budget_dense_rank_2"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary").between(69000, 148000)), "left_semi")
)

result = (
    eligible
    .groupBy("location")
    .agg(
        F.percentile_approx("budget_dense_rank", 0.9).alias("budget_dense_rank_p90"),
        F.round(F.avg("budget_dense_rank_2"), 2).alias("avg_budget_dense_rank_2"),
    )
    .orderBy(F.col("avg_budget_dense_rank_2").desc())
)
