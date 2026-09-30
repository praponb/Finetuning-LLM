from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("hire_date", "id").rowsBetween(-2, -1)
w_hist_2 = Window.partitionBy("dept_id", "level").orderBy(F.col("salary").desc())
w3 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("hire_date", "id")
w5 = Window.partitionBy("level")

valid = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Intern"]))
    .withColumn("level_upper", F.upper("level"))
    .withColumn("email_prefix", F.substring("email", 1, 4))
)

result = (
    valid
    .withColumn("salary_avg_2_prior", F.avg("salary").over(w_hist))
    .withColumn("salary_percent_rank", F.percent_rank().over(w_hist_2))
    .withColumn("cumulative_salary", F.sum("salary").over(w3))
    .withColumn("next_salary", F.lead("salary", 1).over(w4))
    .withColumn("level_count_salary", F.count("salary").over(w5))
)
