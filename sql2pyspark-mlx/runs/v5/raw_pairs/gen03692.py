from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w_part = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").asc_nulls_first())
w3 = Window.partitionBy("dept_name", "location")

matched_keys = (
    df_departments
    .select("id", "location")
    .intersectAll(
        df_departments
        .filter(F.col("location").isin(["Singapore", "Chiang Mai"]))
        .select("id", "location")
    )
)

reconciled = (
    df_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isNull()), "left_anti")
)

scored = (
    not_flagged
    .select(
        "id",
        "dept_name",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.rank().over(w_part).alias("budget_rank"),
        F.stddev_samp("budget").over(w3).alias("dept_name_stddev_samp_budget"),
    )
)

filtered = (
    scored
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("unassigned")))
    .withColumn("greatest_budget_rank_budget_share", F.greatest("budget_rank", "budget_share"))
)

result = (
    filtered
    .select("id", "budget_share", "budget_rank", "dept_name_stddev_samp_budget")
    .orderBy("dept_name_stddev_samp_budget")
)
