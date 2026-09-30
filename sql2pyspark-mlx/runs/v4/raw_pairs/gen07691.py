from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("dept_name").isin(["Finance", "Legal", "HR"]))
    .withColumn("dept_name_location_key", F.concat_ws(",", "dept_name", "location"))
    .withColumn("budget_int", F.col("budget").cast("int"))
    .withColumn("location_clean", F.coalesce("location", F.lit("OTHER")))
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)
