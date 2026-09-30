from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())
w2 = Window.orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") < 2.4)
    .select(
        "id",
        "country",
        "rating",
        F.dense_rank().over(w).alias("rating_dense_rank"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy("country")
)
