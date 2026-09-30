from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([2, 27]) & (F.col("dept_id").isin([6, 11]) | (F.col("salary") >= 115000)))
    .agg(F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"))
    .orderBy(F.col("l1_count").desc())
)
