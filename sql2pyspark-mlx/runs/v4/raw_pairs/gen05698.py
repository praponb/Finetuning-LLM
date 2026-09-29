from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy("hire_date", "id")

deduped = (
    df_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupBy("level")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
    )
    .orderBy(F.col("distinct_manager_ids").desc())
)
