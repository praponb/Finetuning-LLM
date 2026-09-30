from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_first())
w2 = Window.orderBy(F.col("credit_limit").desc(), "id")
w_hist = Window.partitionBy("segment", "country").orderBy(F.col("age").desc(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("credit_limit").asc_nulls_last())

result = (
    df_customers
    .filter(F.col("credit_limit") >= 13500)
    .select(
        "id",
        "name",
        "city",
        "age",
        F.dense_rank().over(w).alias("age_dense_rank"),
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(3).over(w_hist).alias("age_bucket"),
        F.cume_dist().over(w4).alias("credit_limit_cume_dist"),
    )
)
