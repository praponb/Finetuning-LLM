from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("segment")

result = (
    df_customers
    .select(
        "id",
        "country",
        "age",
        "credit_limit",
        "segment",
        F.first("credit_limit").over(w).alias("first_credit_limit"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 3).alias("credit_limit_share"),
    )
    .orderBy(F.col("segment").desc())
)
