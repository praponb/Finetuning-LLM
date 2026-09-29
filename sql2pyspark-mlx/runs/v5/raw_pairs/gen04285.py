from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy("salary", "id")
w3 = Window.partitionBy("dept_id")
w4 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w_hist = Window.orderBy(F.col("salary_bucket").desc(), "id")
w6 = Window.partitionBy("dept_id").orderBy("department_budget", "id")
w7 = Window.partitionBy("department_location", "dept_id")
w_grp = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy("dept_id").orderBy("department_budget_bucket", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_employees
    .filter(F.col("email").like("%@gmail.com"))
    .select(
        "id",
        "full_name",
        "dept_id",
        "salary",
        "hire_date",
        F.first("level", ignorenulls=True).over(w).alias("first_known_level"),
        F.ntile(10).over(w2).alias("salary_bucket"),
    )
)

metrics = (
    enriched
    .withColumn("dept_id_count_salary_bucket", F.count("salary_bucket").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("salary_bucket_bucket", F.ntile(3).over(w_hist))
)

joined = (
    metrics
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.full_name",
        "s.dept_id",
        "s.hire_date",
        "s.first_known_level",
        "s.seq_no",
        F.col("b.dept_name"),
        F.col("b.location"),
        F.col("b.budget"),
    )
)

joined_employees = (
    joined
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.full_name",
        "src.dept_id",
        "src.budget",
        F.col("p.budget").alias("department_budget"),
        F.col("p.location").alias("department_location"),
    )
)

scored = (
    joined_employees
    .select(
        "id",
        "dept_id",
        "budget",
        "department_budget",
        "department_location",
        F.ntile(4).over(w6).alias("department_budget_bucket"),
        F.round((F.col("budget") / F.sum("budget").over(w7)), 4).alias("budget_share"),
    )
)

scored_employees = (
    scored
    .withColumn("first_known_budget", F.first("budget", ignorenulls=True).over(w_grp))
    .withColumn("department_budget_bucket_avg_6", F.avg("department_budget_bucket").over(w_grp_2))
)

base = (
    scored_employees
    .filter(F.col("department_location").isNotNull() & (~F.col("department_location").eqNullSafe(F.lit("Tokyo"))))
    .select(
        "*",
        F.round((F.col("budget") * 1.1), 2).alias("budget_scaled"),
        F.when(F.col("department_budget_bucket_avg_6") >= 9, "High").when(F.col("department_budget_bucket_avg_6") >= 3, "Medium").otherwise("Low").alias("department_budget_bucket_avg_6_level"),
        F.coalesce("department_location", F.lit("NONE")).alias("department_location_filled"),
    )
)

result = (
    base
    .withColumn("department_budget_bucket_avg_6_tier", F.when(F.col("department_budget_bucket_avg_6") >= 7, "HIGH").otherwise("LOW"))
    .withColumn("department_budget_tier", F.when(F.col("department_budget") >= 3348000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["department_budget_bucket_avg_6_tier", "department_budget_tier"], ["department_budget_bucket_avg_6_tier"], []],
        "department_budget_bucket_avg_6_tier",
        "department_budget_tier",
    )
    .agg(
        F.count(F.when(F.col("department_location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.percentile_approx("department_budget_bucket_avg_6", [0.25, 0.5, 0.75]).alias("department_budget_bucket_avg_6_quartiles"),
        F.sort_array(F.collect_set("department_location")).alias("department_location_list"),
        F.grouping("department_budget_bucket_avg_6_tier").alias("gid"),
    )
    .orderBy(F.col("chiang_mai_count").asc_nulls_last())
)
