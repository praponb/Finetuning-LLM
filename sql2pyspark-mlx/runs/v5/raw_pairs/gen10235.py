from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("country", "segment").orderBy("age", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("city")

result = (
    df_customers
    .select(
        "id",
        "email",
        "country",
        "city",
        "is_active",
        F.ntile(5).over(w).alias("credit_limit_bucket"),
        F.avg("age").over(w2).alias("age_avg_5_prior"),
        F.corr("age", "credit_limit").over(w3).alias("corr_age_credit_limit"),
    )
)
