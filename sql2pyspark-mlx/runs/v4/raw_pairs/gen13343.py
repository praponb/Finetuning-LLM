from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_last(), "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget_filled").desc(), "id")
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(F.col("dept_name").isNull())
    .withColumn("location_upper", F.upper("location"))
)

eligible = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id").isNotNull()), "left_semi")
)

filtered = (
    eligible
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "dept_name",
        "budget",
        "location_upper",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("resolved_dept_name"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    deduped
    .withColumn("budget_bucket", F.ntile(5).over(w2))
    .withColumn("budget_filled_bucket", F.ntile(10).over(w3))
    .withColumn("first_known_dept_name", F.first("dept_name", ignorenulls=True).over(w4))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("budget_bucket") >= 9, "HIGH").otherwise("LOW").alias("budget_bucket_tier"),
        F.when(F.col("budget") >= 4488000, "HIGH").otherwise("LOW").alias("budget_tier"),
        "dept_name",
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.round(F.sum("budget_filled") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("budget_filled_per_engineering"),
    )
    .orderBy(F.col("budget_filled_per_engineering").desc())
)
