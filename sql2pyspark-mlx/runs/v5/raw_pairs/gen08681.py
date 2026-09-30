from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "salary",
        F.percent_rank().over(w).alias("salary_pct_rank"),
    )
    .orderBy(F.col("dept_id").desc())
)
