from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w2 = Window.partitionBy("manager_id")
w3 = Window.partitionBy("level").orderBy("salary")

result = (
    df_employees
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("manager_id_min_salary", F.min("salary").over(w2))
    .withColumn("salary_cume_dist", F.cume_dist().over(w3))
)
