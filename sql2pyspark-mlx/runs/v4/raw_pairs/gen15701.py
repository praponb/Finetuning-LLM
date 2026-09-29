from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit")
w2 = Window.orderBy("credit_limit", "id")

result = (
    df_customers
    .select(
        "id",
        "credit_limit",
        F.dense_rank().over(w).alias("credit_limit_dense_rank"),
        F.row_number().over(w2).alias("row_num"),
    )
    .orderBy("credit_limit_dense_rank")
)
