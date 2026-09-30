from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.col("credit_limit") <= 6600)
    .select(
        "id",
        "city",
        "credit_limit",
        F.ntile(4).over(w).alias("credit_limit_ntile"),
    )
)
