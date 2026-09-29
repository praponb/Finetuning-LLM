from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget_scaled", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.round((F.col("budget") * 100), 2).alias("budget_scaled"),
        F.substring("dept_name", 1, 4).alias("dept_name_prefix"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.last("location", ignorenulls=True).over(w).alias("last_known_location"),
        F.avg("budget_scaled").over(w2).alias("budget_scaled_avg_3"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.month(F.col("x.hire_date")).isin([5, 7, 10])), "left_semi")
)

totals = (
    not_flagged
    .withColumn("budget_tier", F.when(F.col("budget") >= 3415000, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["dept_name", "budget_tier"], ["dept_name"], []],
        "dept_name",
        "budget_tier",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum("budget_scaled_avg_3").alias("total_budget_scaled_avg_3"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg(F.when(F.col("location") == "Tokyo", F.col("budget_scaled"))).alias("tokyo_avg_budget_scaled"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    totals
    .select("dept_name", "row_count", "distinct_ids", "tokyo_avg_budget_scaled")
    .orderBy(F.col("distinct_ids").desc(), "tokyo_avg_budget_scaled")
)
