from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last())

result = (
    df_suppliers
    .select(
        "*",
        F.avg("rating").over(w).alias("rating_avg_5"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("rating").over(w3).alias("rating_avg_2_prior"),
        F.percent_rank().over(w4).alias("rating_percent_rank"),
    )
)
