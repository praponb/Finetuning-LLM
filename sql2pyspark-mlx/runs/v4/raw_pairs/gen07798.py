from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-5, -1)
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-05-26").cast("date"))
    .withColumn("email_level_key", F.concat_ws("-", "email", "level"))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
)

result = (
    base
    .withColumn("salary_avg_5_prior", F.avg("salary").over(w))
    .withColumn("cumulative_salary", F.sum("salary").over(w2))
    .withColumn("salary_avg_2", F.avg("salary").over(w3))
)
