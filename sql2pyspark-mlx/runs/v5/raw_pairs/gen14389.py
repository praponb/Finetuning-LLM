from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

not_flagged = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.length(F.col("ch.full_name")) < 6), "left_semi")
)

cleaned = (
    not_flagged
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.when(F.col("budget") >= 4393000, "High").when(F.col("budget") >= 764000, "Medium").otherwise("Low").alias("budget_tier"),
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"),
    )
)

base = (
    cleaned
    .filter(~F.col("dept_name").eqNullSafe(F.lit("Sales")))
    .withColumn("budget_tier_2", F.when(F.col("budget") >= 3065000, "High").when(F.col("budget") >= 1996000, "Medium").otherwise("Low"))
)

scored = (
    base
    .filter(F.col("budget") > 1645000)
    .select(
        "id",
        "dept_name",
        "budget_tier",
        "budget_tier_2",
        F.last("dept_name", ignorenulls=True).over(w).alias("last_known_dept_name"),
        F.ntile(10).over(w2).alias("budget_bucket"),
    )
)

result = (
    scored
    .groupBy("dept_name")
    .agg(
        F.round(F.avg("budget_bucket"), 2).alias("avg_budget_bucket"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
