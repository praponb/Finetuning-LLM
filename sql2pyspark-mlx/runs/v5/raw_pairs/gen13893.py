from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")

result = (
    df_employees
    .filter(F.col("hire_date") <= "2025-09-28")
    .select(
        "id",
        "job_title",
        "salary",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
    )
    .orderBy(F.col("salary_share").desc(), F.col("salary").desc())
)
