from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .select(
        "id",
        "segment",
        "credit_limit",
        F.rank().over(w).alias("credit_limit_rank"),
    )
)
