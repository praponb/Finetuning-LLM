from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id")
w2 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_employees
    .select(
        "id",
        "level",
        "dept_id",
        "salary",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
        F.last("dept_id").over(w2).alias("last_dept_id"),
    )
    .orderBy(F.col("dept_id").desc())
)
