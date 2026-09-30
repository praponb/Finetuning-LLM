from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")

result = (
    df_customers
    .filter(F.col("credit_limit") <= 8800)
    .select(
        "id",
        "segment",
        "credit_limit",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 3).alias("credit_limit_share"),
    )
)
