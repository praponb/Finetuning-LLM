from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc(), "id")
w_running = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment", "city").orderBy("credit_limit", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.ntile(10).over(w).alias("age_bucket"),
        F.first("segment", ignorenulls=True).over(w_running).alias("first_known_segment"),
        F.avg("age").over(w3).alias("age_avg_3"),
        F.last("credit_limit", ignorenulls=True).over(w4).alias("last_known_credit_limit"),
        F.sum("credit_limit").over(w5).alias("running_credit_limit"),
    )
)
