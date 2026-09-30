from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")
w2 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .select(
        "*",
        F.ntile(10).over(w).alias("credit_limit_bucket"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
