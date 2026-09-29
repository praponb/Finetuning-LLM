from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .orderBy(F.col("salary").desc(), "id")
    .limit(1)
)
