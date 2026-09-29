from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "age",
        "email",
        F.last("email").over(w).alias("last_email"),
        F.row_number().over(w2).alias("row_num"),
    )
    .orderBy(F.col("city").desc())
)
