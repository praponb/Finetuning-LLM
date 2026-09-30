from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("job_title", "dept_id").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("level").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("dept_id").isin([12, 39]))
    .select(
        "id",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.stddev_samp("salary").over(w).alias("dept_id_stddev_samp_salary"),
        F.ntile(10).over(w2).alias("salary_bucket"),
        F.ntile(4).over(w3).alias("salary_bucket_2"),
    )
)
