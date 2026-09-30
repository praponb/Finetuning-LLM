from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & (F.col("salary") > 90000) & F.col("email").isNotNull())
    .withColumn("email_upper", F.upper("email"))
    .withColumn("salary_int", F.col("salary").cast("int"))
)
