from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2444000, 3342000))
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy("dept_name")
)
