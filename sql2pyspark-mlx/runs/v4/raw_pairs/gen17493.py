from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com"))
    .select("id", "salary", F.row_number().over(w).alias("row_num"))
)
