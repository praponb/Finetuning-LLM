from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country", "city").orderBy("rating")
w3 = Window.orderBy(F.col("rating").asc_nulls_first())

scored = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        F.ntile(3).over(w).alias("rating_bucket"),
        F.dense_rank().over(w2).alias("rating_dense_rank"),
        F.rank().over(w3).alias("rating_rank"),
    )
)

result = (
    scored
    .select("id", "rating", "rating_bucket", "rating_dense_rank", "rating_rank")
    .orderBy(F.col("rating_rank").asc_nulls_last())
)
