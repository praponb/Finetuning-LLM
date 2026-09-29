from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy("budget", "id")

filtered = (
    df_departments
    .filter(F.col("dept_name") != "HR")
    .select("*", F.upper("location").alias("location_upper"))
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w_recent).alias("row_seq"),
        F.last("location", ignorenulls=True).over(w2).alias("last_known_location"),
        F.row_number().over(w3).alias("row_seq_2"),
    )
)
