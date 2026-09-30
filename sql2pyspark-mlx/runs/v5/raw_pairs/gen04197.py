from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

ranked = (
    df_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_name")
)

result = (
    ranked
    .groupBy("dept_name")
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("dept_name") == "HR").alias("hr_events"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("dept_name") == "Sales").alias("sales_events"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names_2"),
    )
    .orderBy("row_count")
)
