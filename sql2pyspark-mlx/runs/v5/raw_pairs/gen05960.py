from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter((F.col("location").eqNullSafe(F.lit("Berlin"))) & F.col("location").isin(["Chiang Mai", "Berlin"]))
    .select(
        "*",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
        F.concat("location", F.lit(" - "), "dept_name").alias("location_dept_name"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"))
)
