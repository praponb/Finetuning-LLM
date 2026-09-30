from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .groupBy(F.year("hire_date").alias("hire_year"))
    .pivot("level", ["L5", "L2"])
    .agg(F.max("salary"))
    .orderBy("hire_year")
)
