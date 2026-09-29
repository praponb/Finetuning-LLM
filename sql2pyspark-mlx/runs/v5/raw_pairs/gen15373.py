from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc())
w2 = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "job_title",
        "hire_date",
        "level",
        "salary",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
)
