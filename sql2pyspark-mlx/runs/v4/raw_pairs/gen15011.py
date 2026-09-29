from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
)
