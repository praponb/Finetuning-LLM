from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 1)
)
