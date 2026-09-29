from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull() & F.lower("email").like("%lic%") & (F.col("hire_date") <= "2024-08-13"))
    .groupBy("level")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
    )
    .orderBy("level")
)
