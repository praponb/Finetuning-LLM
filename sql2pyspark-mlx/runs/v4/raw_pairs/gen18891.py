from pyspark.sql import functions as F

result = (
    df_departments
    .withColumn("budget_rounded", F.round("budget", 0))
    .withColumn("dept_name_length", F.length("dept_name"))
)
