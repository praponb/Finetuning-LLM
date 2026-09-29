from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull())
    .withColumn("level_group", F.when(F.col("level").isin(["L4", "L1"]), "Priority").otherwise("Other"))
    .withColumn("level_clean", F.coalesce("level", F.lit("DIRECT")))
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
