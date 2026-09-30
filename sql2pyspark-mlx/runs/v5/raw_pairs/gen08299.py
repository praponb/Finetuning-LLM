from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level", "manager_id").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w_hist = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
        F.avg("salary").over(w_hist).alias("salary_avg_5_prior"),
        F.sum("salary").over(w5).alias("running_salary"),
    )
)
