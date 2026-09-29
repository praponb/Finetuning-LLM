from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("job_title").orderBy("hire_date", "id")

scored = (
    df_employees
    .withColumn("next_salary", F.lead("salary").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

result = scored.select("id", "full_name", "hire_date", "is_remote", "level", "next_salary")
