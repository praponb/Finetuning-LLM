from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.month("signup_date").isin([2, 3, 4]))
    .select("id", "segment", "credit_limit", F.row_number().over(w).alias("rn"))
)
