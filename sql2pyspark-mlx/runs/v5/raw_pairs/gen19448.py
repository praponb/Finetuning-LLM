from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id")
w2 = Window.partitionBy("city").orderBy("age", "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "credit_limit",
        "city",
        "age",
        F.row_number().over(w).alias("credit_limit_row"),
        F.row_number().over(w2).alias("rn"),
    )
)
