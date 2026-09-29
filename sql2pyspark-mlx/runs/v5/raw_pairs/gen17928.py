from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "level").orderBy("hire_date")

result = (
    df_employees
    .filter(F.col("email").like("%@yahoo.com"))
    .select(
        "id",
        "dept_id",
        "level",
        "hire_date",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
