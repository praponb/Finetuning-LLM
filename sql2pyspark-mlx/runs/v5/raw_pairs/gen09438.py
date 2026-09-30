from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("level").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("level", "job_title").orderBy("hire_date", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("level", "manager_id").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)

metrics = (
    df_employees
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("salary_percent_rank", F.percent_rank().over(w2))
    .withColumn("salary_avg_2_prior", F.avg("salary").over(w3))
    .withColumn("salary_avg_6", F.avg("salary").over(w4))
)

result = (
    metrics
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "row_seq",
        "salary_percent_rank",
        "salary_avg_2_prior",
        "salary_avg_6",
        F.when(F.col("full_name").rlike("^(Ivy Chen|Emma Brown)$"), "EXTENDED").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("full_name_zone"),
    )
)
