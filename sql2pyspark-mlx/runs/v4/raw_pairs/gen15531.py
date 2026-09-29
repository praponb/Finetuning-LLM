from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_last())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("location").isin(["New York", "Berlin", "Chiang Mai"]))
    .select(
        "*",
        F.round((F.col("budget") * 1.07), 2).alias("budget_adjusted"),
        F.lower("dept_name").alias("dept_name_lower"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)
