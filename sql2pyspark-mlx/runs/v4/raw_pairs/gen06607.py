from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "salary",
        "hire_date",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
        (F.col("salary") - F.lag("salary", 2).over(w2)).alias("salary_change"),
    )
)
