from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .filter(F.month("signup_date").isin([1, 4, 9]))
    .select("id", "city", "age", F.row_number().over(w).alias("rn"))
)
