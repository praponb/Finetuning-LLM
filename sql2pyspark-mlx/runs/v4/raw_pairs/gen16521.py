from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())
w2 = Window.orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.dense_rank().over(w).alias("budget_dense_rank"),
        F.ntile(2).over(w2).alias("budget_ntile"),
    )
)
