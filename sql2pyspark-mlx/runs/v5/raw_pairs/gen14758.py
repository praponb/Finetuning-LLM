from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_last())
w3 = Window.partitionBy("segment", "country").orderBy("signup_date", "id")
w4 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .filter(F.col("age") <= 32)
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "credit_limit",
        "age",
        F.sum("age").over(w).alias("running_age"),
        F.dense_rank().over(w2).alias("credit_limit_dense_rank"),
        F.lag("age", 1, 0).over(w3).alias("prev_age"),
        F.first("age", ignorenulls=True).over(w4).alias("first_known_age"),
    )
)
