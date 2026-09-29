from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("manager_id").orderBy("salary", "id")
w_part_2 = Window.partitionBy("level")
w3 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.lag("salary", 1).over(w_part).alias("prev_salary"),
        F.stddev_samp("salary").over(w_part_2).alias("level_stddev_samp_salary"),
        F.first("level", ignorenulls=True).over(w3).alias("first_known_level"),
        F.stddev_samp("salary").over(w_part_2).alias("level_stddev_samp_salary_2"),
    )
)
