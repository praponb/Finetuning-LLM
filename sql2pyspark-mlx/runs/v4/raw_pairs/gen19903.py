from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.job_title"))
)

result = (
    latest_match
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "level",
        "job_title",
        F.when(F.col("dept_name").rlike("^(Finance|Sales)$"), "GROUP_B").when(F.col("dept_name").rlike("^(Engineering|Marketing)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("dept_name_zone"),
    )
    .orderBy(F.col("job_title").desc())
)
