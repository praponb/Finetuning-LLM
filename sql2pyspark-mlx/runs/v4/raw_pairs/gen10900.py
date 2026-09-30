from pyspark.sql import functions as F

src = df_employees.filter(F.col("level") == "L4")

agg = (
    src
    .groupBy("is_remote")
    .agg(F.sum("salary").alias("total_salary"))
)

result = (
    agg
    .filter(F.col("total_salary") > 485000)
    .orderBy("total_salary", "is_remote")
    .limit(10)
)
