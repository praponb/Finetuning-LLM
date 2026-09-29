from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy("budget", "id")

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_level", F.when(F.col("budget") >= 3908000, "High").when(F.col("budget") >= 675000, "Medium").otherwise("Low"))
    .withColumn("location_dept_name_key", F.concat_ws("/", "location", "dept_name"))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("DIRECT")))
)

with_windows = (
    cleaned
    .withColumn("next_budget", F.lead("budget", 1, 0).over(w))
    .withColumn("prev_budget", F.lag("budget", 1, 0).over(w2))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary") < 84000), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "dept_name",
        "budget",
        "budget_level",
        "location_dept_name_key",
        "dept_name_clean",
        "prev_budget",
    )
    .orderBy(F.col("budget").asc_nulls_first(), F.col("prev_budget").desc())
)
