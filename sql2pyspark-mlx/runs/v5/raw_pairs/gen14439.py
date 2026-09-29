from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date")

result = (
    df_employees
    .select(
        "id",
        "level",
        "hire_date",
        "manager_id",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "level", "hire_date", "manager_id")
)
