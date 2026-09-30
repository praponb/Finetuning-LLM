from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w_ordered = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-3, Window.currentRow)
w_ordered_2 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.avg("salary").over(w_ordered).alias("salary_avg_3"),
        F.first("job_title", ignorenulls=True).over(w_ordered_2).alias("first_known_job_title"),
        F.rank().over(w4).alias("salary_rank"),
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share_2"),
    )
)
