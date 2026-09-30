from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit")

result = (
    df_customers
    .filter(F.col("city").isin(["Singapore", "Bangkok", "London"]))
    .select(
        "id",
        "country",
        "credit_limit",
        F.dense_rank().over(w).alias("credit_limit_dense_rank"),
    )
)
