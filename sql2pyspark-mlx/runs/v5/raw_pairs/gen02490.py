from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")

result = (
    df_employees
    .select("id", "level", "salary", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy("salary", F.col("rn").desc())
)
