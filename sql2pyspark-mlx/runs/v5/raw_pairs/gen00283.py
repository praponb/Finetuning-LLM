from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_recent = Window.orderBy(F.col("rating").desc_nulls_first())

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Parts%"))
    .select(
        "*",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.dense_rank().over(w_recent).alias("rating_dense_rank"),
    )
)
