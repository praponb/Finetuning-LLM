from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("level", "dept_id").orderBy("salary", "id")
w3 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-3, -1)

result = (
    df_employees
    .withColumn("salary_avg_5_prior", F.avg("salary").over(w))
    .withColumn("prev_salary", F.lag("salary", 2).over(w2))
    .withColumn("salary_avg_3_prior", F.avg("salary").over(w3))
)
