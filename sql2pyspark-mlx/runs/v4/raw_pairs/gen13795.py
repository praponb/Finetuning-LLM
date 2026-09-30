from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age")
w2 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "age",
        "segment",
        F.rank().over(w).alias("rnk"),
        F.row_number().over(w2).alias("age_row"),
    )
)
