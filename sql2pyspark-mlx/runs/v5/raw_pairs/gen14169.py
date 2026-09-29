from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
    )
)
