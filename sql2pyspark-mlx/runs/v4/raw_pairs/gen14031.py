from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

latest = (
    df_departments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "dept_name",
        "budget",
        "rank_in_group",
        F.when(F.col("dept_name").rlike("^(Engineering|Legal)$"), "CORE").when(F.col("dept_name").rlike("^(Sales|Marketing)$"), "EXTENDED").otherwise("PRIORITY").alias("dept_name_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
