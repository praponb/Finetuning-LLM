from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w_part_2 = Window.orderBy(F.col("rating").desc())
w_running = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first())
w_recent = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "*",
        F.avg("rating").over(w_part).alias("rating_avg_2"),
        F.dense_rank().over(w_part_2).alias("rating_dense_rank"),
        F.rank().over(w_running).alias("rating_rank"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)
