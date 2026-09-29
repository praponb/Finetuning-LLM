from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([5, 10, 17, 34]) & (F.col("salary") > 62000) & (F.col("manager_id") == 30))
    .groupBy("is_remote")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
    )
)
