from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy("salary_whole", "id")
w3 = Window.partitionBy("job_title", "dept_id").orderBy("salary_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2024-03-20").cast("date")) & F.col("level").isNotNull() & (F.year("hire_date") >= 2025))
    .withColumn("salary_whole", F.ceil("salary"))
)

result = (
    base
    .withColumn("salary_whole_avg_6", F.avg("salary_whole").over(w))
    .withColumn("prev_salary", F.lag("salary", 2).over(w2))
    .withColumn("running_salary_whole", F.sum("salary_whole").over(w3))
)
