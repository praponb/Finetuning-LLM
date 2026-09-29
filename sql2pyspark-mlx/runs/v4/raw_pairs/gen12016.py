from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id")
w2 = Window.orderBy("credit_limit")

result = (
    df_customers
    .select(
        "id",
        "country",
        "credit_limit",
        F.row_number().over(w).alias("credit_limit_row"),
        F.rank().over(w2).alias("credit_limit_rank"),
    )
    .orderBy("credit_limit_row", "credit_limit")
)
