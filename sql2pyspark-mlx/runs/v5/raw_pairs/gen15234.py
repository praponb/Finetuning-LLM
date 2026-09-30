from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit")

result = (
    df_customers
    .select(
        "id",
        "country",
        "credit_limit",
        F.percent_rank().over(w).alias("credit_limit_pct_rank"),
    )
    .orderBy("credit_limit_pct_rank", F.col("credit_limit").desc())
)
