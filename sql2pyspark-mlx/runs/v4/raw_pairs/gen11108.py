from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id").orderBy("salary", "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-12-09").cast("date")) & ~F.col("is_remote"))
    .select("*", F.length("email").alias("email_length"))
)

result = (
    cleaned
    .withColumn("next_email_length", F.lead("email_length", 1).over(w))
    .withColumn("salary_bucket", F.ntile(5).over(w2))
)
