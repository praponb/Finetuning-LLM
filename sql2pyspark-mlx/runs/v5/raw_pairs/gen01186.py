from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date_period", "id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

valid = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & (~F.col("job_title").eqNullSafe(F.lit("Manager"))))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        F.date_trunc("MONTH", "hire_date").alias("hire_date_period"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level"),
    )
)

prev_events = valid.withColumn("prev_hire_date_period", F.lag("hire_date_period").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_hire_date_period").isNull() | (F.col("hire_date_period") > F.col("prev_hire_date_period") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date_period",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

ranked = (
    islands
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "dept_id", "salary")
)

base = (
    ranked
    .filter(F.col("salary") < 105000)
    .withColumn("salary_rounded", F.round("salary", 0))
    .withColumn("salary_int", F.col("salary").cast("int"))
)

enriched = (
    base
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.dept_name"))
)

joined = (
    enriched
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.dept_id",
        "s.salary",
        "s.salary_int",
        "s.dept_name",
        F.col("p.budget"),
        F.col("p.dept_name").alias("department_dept_name"),
    )
)

joined_employees = (
    joined
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.dept_id",
        "src.dept_name",
        "src.department_dept_name",
        F.col("b.location"),
        F.col("b.dept_name").alias("department_dept_name_2"),
        F.col("b.budget").alias("department_budget"),
    )
)

result = (
    joined_employees
    .select("id", "dept_name", "department_dept_name", "location", "department_budget")
    .orderBy(F.col("department_budget").desc_nulls_last(), "department_dept_name")
)
