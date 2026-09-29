from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())
w2 = Window.partitionBy("job_title").orderBy("salary")

result = (
    df_employees
    .select(
        "id",
        "level",
        "hire_date",
        "job_title",
        "salary",
        F.rank().over(w).alias("rnk"),
        F.dense_rank().over(w2).alias("salary_dense_rank"),
    )
)
