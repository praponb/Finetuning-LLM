from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([21, 26, 37]))
    .groupBy("level")
    .agg(F.countDistinct("salary").alias("unique_managers"))
    .filter(F.col("unique_managers") > 4)
    .orderBy(F.col("unique_managers").desc())
)
