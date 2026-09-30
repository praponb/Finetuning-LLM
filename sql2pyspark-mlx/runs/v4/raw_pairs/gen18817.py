from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"))
)

result = (
    latest_match
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.when(F.col("location").rlike("^(Berlin|New York)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("location_zone"),
    )
    .orderBy(F.col("location_zone").desc_nulls_last(), F.col("location").desc())
)
