from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("city").orderBy(F.col("age").asc_nulls_last())
w3 = Window.partitionBy("segment", "country").orderBy("age", "id")

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .select(
        "id",
        "email",
        "city",
        "segment",
        "is_active",
        F.count("age").over(w).alias("segment_count_age"),
        F.cume_dist().over(w2).alias("age_cume_dist"),
        F.lag("credit_limit", 2, 0).over(w3).alias("prev_credit_limit"),
    )
)
