from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")
w2 = Window.orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.dense_rank().over(w).alias("rating_dense_rank"),
        F.row_number().over(w2).alias("rn"),
    )
)
