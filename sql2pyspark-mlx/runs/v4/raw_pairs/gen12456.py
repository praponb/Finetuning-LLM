from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.count("*").alias("cnt"),
    )
    .orderBy("hire_month")
)
