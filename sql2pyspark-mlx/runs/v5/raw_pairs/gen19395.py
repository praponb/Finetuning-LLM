from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc())
w2 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_customers
    .filter(F.col("country").isin(["US", "DE", "UK"]))
    .select(
        "id",
        "country",
        "age",
        "city",
        "signup_date",
        F.dense_rank().over(w).alias("age_dense_rank"),
        F.round(F.avg("age").over(w2), 2).alias("age_moving_avg_7"),
    )
)
