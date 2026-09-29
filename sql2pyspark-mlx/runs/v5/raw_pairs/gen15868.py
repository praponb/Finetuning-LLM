from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        "job_title",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
        F.ntile(4).over(w2).alias("salary_ntile"),
    )
)
