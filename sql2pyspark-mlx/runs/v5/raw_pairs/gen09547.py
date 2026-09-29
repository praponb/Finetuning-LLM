from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget_whole").desc_nulls_first(), "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_whole", F.ceil("budget"))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("DIRECT")))
)

result = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "dept_name", "budget_whole")
)
