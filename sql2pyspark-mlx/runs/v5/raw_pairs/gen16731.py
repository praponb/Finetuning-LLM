from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 3932000)
    .groupBy("location")
    .pivot("dept_name", ["Legal", "Marketing"])
    .agg(F.avg("budget"))
    .orderBy("location")
)
