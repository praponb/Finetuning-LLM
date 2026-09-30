from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        F.min("salary").over(w).alias("manager_id_min_salary"),
    )
    .orderBy("id")
)
