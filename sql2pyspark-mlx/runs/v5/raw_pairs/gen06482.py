from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .filter(F.length("email") > 10)
    .select(
        "id",
        "city",
        "credit_limit",
        F.dense_rank().over(w).alias("credit_limit_dense_rank"),
    )
    .orderBy(F.col("city").desc())
)
