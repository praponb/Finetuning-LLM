from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("country").orderBy("signup_date")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "age",
        "country",
        "signup_date",
        F.avg("age").over(w).alias("segment_avg_age"),
        F.dense_rank().over(w2).alias("signup_date_dense_rank"),
    )
)
