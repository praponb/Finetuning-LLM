from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title").orderBy("salary")

enriched = (
    df_employees
    .withColumn("prev_salary", F.lag("salary", 2).over(w))
    .withColumn("salary_dense_rank", F.dense_rank().over(w2))
)

result = (
    enriched
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "prev_salary",
        F.when(F.col("job_title").rlike("^(Manager|Engineer)$"), "PRIORITY").when(F.col("job_title").rlike("^(Analyst|Director)$"), "GROUP_A").otherwise("GROUP_B").alias("job_title_zone"),
    )
)
