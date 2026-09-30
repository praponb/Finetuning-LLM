from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .select(
        "*",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.rank().over(w2).alias("budget_rank"),
    )
)
