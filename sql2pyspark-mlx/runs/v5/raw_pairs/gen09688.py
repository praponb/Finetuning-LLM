from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Sales", "Finance"]))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.first("dept_name").over(w).alias("first_dept_name"),
    )
)
