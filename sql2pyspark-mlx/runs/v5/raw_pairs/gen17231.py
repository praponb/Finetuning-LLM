from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("level").isin(["L4", "L3", "L2"]))
    .agg(
        F.count("*").alias("row_count"),
        F.max("hire_date").alias("latest_hire_date"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .orderBy(F.col("salary_range").desc(), F.col("row_count").desc())
)
