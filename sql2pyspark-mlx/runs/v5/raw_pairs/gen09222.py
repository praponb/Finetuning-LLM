from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy(F.col("hire_date").desc_nulls_first(), "id")

result = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_id", "job_title", "hire_date", "is_remote", "level")
)
