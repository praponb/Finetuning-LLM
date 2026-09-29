from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id")
w2 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city", "country").orderBy("credit_limit", "id")
w_ordered = Window.partitionBy("segment", "country").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.ntile(3).over(w).alias("age_bucket"),
        F.sum("age").over(w2).alias("running_age"),
        F.lag("age", 2, 0).over(w_running).alias("prev_age"),
        F.ntile(5).over(w_ordered).alias("age_bucket_2"),
    )
)
