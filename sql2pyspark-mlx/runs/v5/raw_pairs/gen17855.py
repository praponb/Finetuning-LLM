from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_last())
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.dense_rank().over(w).alias("rating_dense_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
        F.ntile(5).over(w3).alias("rating_bucket"),
    )
)
