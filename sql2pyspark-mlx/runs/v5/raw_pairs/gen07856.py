from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-10-02", "2024-11-21"))
    .select(
        "id",
        "manager_id",
        "salary",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
    )
)
