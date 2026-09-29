from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "Legal"]))
    .select("*", F.trim("dept_name").alias("clean_dept_name"))
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"))
)

result = (
    with_latest
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "clean_dept_name",
        "rank_in_group",
        "level",
    )
    .orderBy(F.col("rank_in_group").desc_nulls_first(), "level")
)
