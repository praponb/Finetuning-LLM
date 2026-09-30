from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget_int")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Marketing", "Legal"]))
    .select(
        "*",
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("dept_name_clean"),
    )
)

ranked = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget_int",
        "s.dept_name_clean",
        "s.row_num",
        F.col("c.hire_date"),
        F.col("c.salary"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") > "2025-02-01"), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("row_num") == 1)
    .withColumn("row_num_str", F.col("row_num").cast("double"))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

result = (
    filtered
    .groupBy("dept_name")
    .pivot("location", ["Berlin", "New York"])
    .agg(F.avg("budget_int"))
    .select(
        "dept_name",
        F.col("Berlin").alias("berlin"),
        F.col("New York").alias("new_york"),
    )
)
