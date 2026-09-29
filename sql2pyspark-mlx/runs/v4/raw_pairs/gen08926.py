from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.orderBy(F.col("budget_int").desc(), "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Marketing"]))
    .withColumn("budget_int", F.col("budget").cast("int"))
)

result = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
    )
)
