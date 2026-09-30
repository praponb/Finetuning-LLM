from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")
w_grp = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy("salary")

result = (
    df_employees
    .filter(F.col("salary").between(100000, 106000))
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.first("level", ignorenulls=True).over(w_grp).alias("first_known_level"),
        F.cume_dist().over(w3).alias("salary_cume_dist"),
    )
)
