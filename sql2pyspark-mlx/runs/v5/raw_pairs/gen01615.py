from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first(), "id")

ranked = (
    df_departments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "dept_name",
        "budget",
        "latest_rank",
        F.when(F.col("dept_name").rlike("^(Finance|Engineering)$"), "GROUP_A").when(F.col("dept_name").rlike("^(Sales|Marketing)$"), "CORE").otherwise("OTHER_ZONE").alias("dept_name_zone"),
    )
    .orderBy("id", "latest_rank")
)
