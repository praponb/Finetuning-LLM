from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .filter(F.col("credit_limit").between(11500, 14100))
    .select(
        "id",
        "city",
        "country",
        "credit_limit",
        F.rank().over(w).alias("credit_limit_rank"),
    )
)
