from pyspark.sql import functions as F

result = (
    df_departments
    .withColumn("dept_name_length", F.length("dept_name"))
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["HR", "Marketing"]), "Core").otherwise("Other"))
    .orderBy("id")
    .limit(20)
)
