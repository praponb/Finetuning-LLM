from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .select("id", "level", "salary", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "level", "salary")
)
