from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("manager_id")

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "job_title",
        "salary",
        "manager_id",
        F.dense_rank().over(w).alias("salary_dense_rank"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 3).alias("salary_share"),
    )
)
