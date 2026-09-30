from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

deduped = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "rank_in_group",
        F.when(F.col("level").rlike("^(L3|L5)$"), "CORE").when(F.col("level").rlike("^(L1|L2)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("level_zone"),
    )
    .orderBy(F.col("job_title").desc(), F.col("manager_id").desc())
)
