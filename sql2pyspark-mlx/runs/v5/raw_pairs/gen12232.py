from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(F.col("job_title") == "Designer")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "dept_id", "hire_date", "is_remote")
    .orderBy(F.col("dept_id").desc())
)
