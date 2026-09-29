from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")
w2 = Window.orderBy("budget_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_departments
    .filter(F.col("budget") <= 2370000)
    .select(
        "*",
        F.round((F.col("budget") * 100), 2).alias("budget_scaled"),
        F.coalesce("location", F.lit("OTHER")).alias("resolved_location"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.full_name").isNotNull()), "left_anti")
)

enriched = (
    kept
    .withColumn("location_max_budget", F.max("budget").over(w))
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w2))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("budget_scaled") >= 83, "LARGE").otherwise("SMALL").alias("budget_scaled_tier"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget_scaled")).otherwise(0)).alias("finance_budget_scaled"),
        F.count("*").alias("event_count"),
        F.percentile_approx("location_max_budget", [0.25, 0.5, 0.75]).alias("location_max_budget_quartiles"),
    )
    .orderBy(F.col("event_count").asc_nulls_first())
)
