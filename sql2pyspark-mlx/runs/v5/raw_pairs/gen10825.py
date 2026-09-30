from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Engineering"]) & F.col("location").isin(["Tokyo", "London", "New York"]))
    .withColumn("is_sales", F.when(F.col("dept_name") == "Sales", 1).otherwise(0))
    .orderBy(F.col("is_sales").desc(), "id")
    .limit(5)
)
