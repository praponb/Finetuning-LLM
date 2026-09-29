from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("job_title", "manager_id")
w3 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_employees
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.sum("salary").over(w3).alias("running_salary"),
    )
)

result = (
    metrics
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "row_seq",
        F.when(F.col("level").rlike("^(L1|L2)$"), "GROUP_A").when(F.col("level").rlike("^(L3|L4)$"), "EXTENDED").otherwise("GROUP_B").alias("level_zone"),
    )
)
