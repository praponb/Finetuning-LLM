from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("location", "level").orderBy(F.col("employee_salary").asc_nulls_first())

cleaned = (
    df_departments
    .filter(F.col("dept_name") == "Engineering")
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" "), "location"))
    .withColumn("dept_name_title", F.initcap(F.trim("dept_name")))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

with_last = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.salary").alias("employee_salary"))
)

kept_keys = (
    with_last
    .select("id", "location")
    .subtract(
        df_departments
        .filter(~F.col("dept_name").isin(["Marketing", "Finance"]))
        .select("id", "location")
    )
)

kept_rows = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w3)), 4))
    .withColumn("employee_salary_cume_dist", F.cume_dist().over(w4))
)

kept = (
    with_windows
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") >= F.col("s.employee_hire_date")), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") >= F.col("s.employee_hire_date")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_location",
        "dept_name_title",
        "location_clean",
        "employee_hire_date",
        "level",
        "budget_share",
        F.when(F.col("level").rlike("^(L4|L3)$"), "CORE").when(F.col("level").rlike("^(L1|L2)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("level_zone"),
    )
    .orderBy("budget_share")
)
