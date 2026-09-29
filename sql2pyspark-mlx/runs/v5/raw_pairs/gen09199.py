from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
)
