from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.orderBy(F.col("budget").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.first("dept_name").over(w).alias("first_dept_name"),
        F.last("location").over(w2).alias("last_location"),
    )
)
