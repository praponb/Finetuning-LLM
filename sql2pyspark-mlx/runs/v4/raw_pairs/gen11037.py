from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")
w2 = Window.partitionBy("dept_name")

result = (
    df_departments
    .filter(F.col("budget") < 3741000)
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 3).alias("budget_share"),
    )
    .orderBy("dept_name")
)
