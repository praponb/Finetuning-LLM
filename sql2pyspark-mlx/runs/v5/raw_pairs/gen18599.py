from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Bangkok") & (F.col("budget") > 1565000))
    .groupBy("dept_name", "location")
    .agg(F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"))
)
