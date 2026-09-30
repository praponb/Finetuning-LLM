from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_first())
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-5, -1)

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.dense_rank().over(w).alias("rating_dense_rank"),
        F.row_number().over(w2).alias("event_seq"),
        F.avg("rating").over(w3).alias("rating_avg_5_prior"),
    )
)
