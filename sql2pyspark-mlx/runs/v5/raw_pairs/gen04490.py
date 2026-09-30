from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-6, -1)

result = (
    df_employees
    .select(
        "id",
        "email",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.ntile(5).over(w).alias("salary_bucket"),
        F.avg("salary").over(w2).alias("salary_avg_6_prior"),
    )
)
