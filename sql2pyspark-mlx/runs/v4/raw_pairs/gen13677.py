from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary")

deduped = (
    df_employees
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "dedup_rank",
        F.when(F.col("level").rlike("^(L3|L5)$"), "PRIORITY").otherwise("EXTENDED").alias("level_zone"),
    )
    .orderBy("level_zone")
)
