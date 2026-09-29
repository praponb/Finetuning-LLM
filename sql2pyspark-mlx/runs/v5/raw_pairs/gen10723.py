from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("dept_id") == 2)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .orderBy("dept_id")
)
