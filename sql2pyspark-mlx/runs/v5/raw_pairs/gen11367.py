from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget_filled").desc())
w2 = Window.orderBy(F.col("budget_filled").asc_nulls_last())
w3 = Window.partitionBy("location")
w4 = Window.partitionBy("dept_name")

remaining_keys = (
    df_departments
    .select("dept_name")
    .intersect(
        df_departments
        .filter(F.col("dept_name") == "Legal")
        .select("dept_name")
    )
)

kept_rows = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(~F.col("location").eqNullSafe(F.lit("Berlin")))
    .select("*", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.full_name").like("Ja%")), "left_semi")
)

metrics = (
    eligible
    .withColumn("budget_filled_dense_rank", F.dense_rank().over(w))
    .withColumn("budget_filled_rank", F.rank().over(w2))
)

scored = (
    metrics
    .select(
        "id",
        "dept_name",
        F.dense_rank().over(w2).alias("budget_filled_dense_rank_2"),
        F.round((F.col("budget_filled_rank") / F.sum("budget_filled_rank").over(w3)), 4).alias("budget_filled_rank_share"),
        F.stddev_samp("budget_filled_dense_rank").over(w4).alias("dept_name_stddev_samp_budget_filled_dense_rank"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.dept_id").isin([11, 37, 40])), "left_anti")
)

scored_departments = (
    not_flagged
    .withColumn("budget_filled_dense_rank_2_share", F.round((F.col("budget_filled_dense_rank_2") / F.sum("budget_filled_dense_rank_2").over(w4)), 4))
    .withColumn("dept_name_stddev_samp_budget_filled_rank_share", F.stddev_samp("budget_filled_rank_share").over(w4))
)

stats = (
    scored_departments
    .groupBy("dept_name")
    .agg(
        F.max(F.when(F.col("dept_name") == "Engineering", F.col("budget_filled_dense_rank_2_share"))).alias("engineering_max_budget_filled_dense_rank_2_share"),
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("budget_filled_dense_rank_2")).otherwise(0)).alias("engineering_budget_filled_dense_rank_2"),
    )
)

result = stats.select("dept_name")
