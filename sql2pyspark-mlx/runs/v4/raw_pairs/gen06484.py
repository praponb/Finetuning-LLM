from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.orderBy(F.col("age").desc())

result = (
    df_customers
    .select(
        "id",
        "credit_limit",
        "age",
        F.row_number().over(w).alias("row_num"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("row_num").desc())
)
