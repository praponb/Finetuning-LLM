from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_year")

result = (
    df_employees
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
)
