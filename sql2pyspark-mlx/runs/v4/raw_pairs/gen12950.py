from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())

base = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-10-08").cast("date"))
    .withColumn("hire_date_period", F.date_trunc("YEAR", "hire_date"))
    .withColumn("level_clean", F.coalesce("level", F.lit("DIRECT")))
)

result = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
