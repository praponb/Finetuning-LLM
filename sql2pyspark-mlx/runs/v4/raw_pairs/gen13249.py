from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w_ordered = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .withColumn("salary_bucket", F.ntile(3).over(w))
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w_ordered))
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w3))
)
