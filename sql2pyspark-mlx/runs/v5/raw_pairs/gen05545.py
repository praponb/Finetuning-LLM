from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.upper("dept_name").alias("dept_name_upper"),
        F.initcap(F.trim("location")).alias("location_title"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

filtered = (
    deduped
    .filter(F.col("location") != "Bangkok")
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_upper",
        "location_title",
        "rn",
        (F.col("budget") + F.col("rn")).alias("budget_plus_rn"),
        (F.col("budget") * F.col("rn")).alias("budget_times_rn"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary").between(74000, 91000)), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (~F.col("ch.is_remote")), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.location_title",
        "s.budget_plus_rn",
        "s.budget_times_rn",
        F.col("c.level").alias("employee_level"),
        F.col("c.salary"),
    )
)

valid = (
    with_last
    .filter(F.col("location").isin(["Chiang Mai", "Singapore", "Tokyo"]))
    .select(
        "*",
        (F.col("salary") - F.col("budget_plus_rn")).alias("salary_minus_budget_plus_rn"),
    )
)

wide = (
    valid
    .groupBy("dept_name", "location")
    .pivot("employee_level", ["L1", "L2"])
    .agg(F.avg("budget_times_rn").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "dept_name",
        "location",
        F.col("L1_value_sum").alias("l1_value_sum"),
        F.col("L1_n").alias("l1_n"),
        F.col("L2_value_sum").alias("l2_value_sum"),
        F.col("L2_n").alias("l2_n"),
    )
)

result = (
    wide
    .select(
        "dept_name",
        "location",
        "l1_value_sum",
        "l1_n",
        F.when(F.col("location").rlike("^(Berlin|Singapore)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("location_zone"),
    )
    .orderBy(F.col("location").asc_nulls_first())
)
