from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc_nulls_first())
w2 = Window.partitionBy("city").orderBy("age", "id")
w3 = Window.partitionBy("segment")

result = (
    df_customers
    .select(
        "*",
        F.cume_dist().over(w).alias("credit_limit_cume_dist"),
        F.ntile(10).over(w2).alias("age_bucket"),
        F.avg("age").over(w3).alias("segment_avg_age"),
    )
)
