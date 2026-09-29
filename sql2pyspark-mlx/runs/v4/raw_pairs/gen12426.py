from pyspark.sql import functions as F

src = df_employees.filter((F.col("salary") <= 105000) | ~F.col("is_remote"))

agg = (
    src
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.countDistinct("salary").alias("unique_levels"))
)

result = agg.select("hire_month", "unique_levels")
