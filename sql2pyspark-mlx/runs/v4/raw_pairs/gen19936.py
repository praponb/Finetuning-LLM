from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("budget_bucket", F.ntile(3).over(w2))
)
