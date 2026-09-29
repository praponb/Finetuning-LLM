from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([4, 15, 21, 35]) & F.col("is_remote"))
    .groupBy("is_remote", "level")
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
    .orderBy(F.col("level").desc(), "is_remote")
    .limit(5)
)
