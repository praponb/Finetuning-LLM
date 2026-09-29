from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
)

agg = (
    filtered_employees
    .groupBy(
        F.when(F.col("level").isin(["L2", "L4"]), "core").otherwise("other").alias("level_group"),
    )
    .agg(F.count("is_remote_flag").alias("is_remote_flag_count"))
)

result = agg.select("level_group", "is_remote_flag_count")
