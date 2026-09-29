from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")
w4 = Window.partitionBy("segment").orderBy("age", "id")

with_windows = (
    df_customers
    .select(
        "*",
        F.last("age", ignorenulls=True).over(w).alias("last_known_age"),
        F.sum("age").over(w2).alias("running_age"),
        F.row_number().over(w3).alias("event_seq"),
        F.lag("age", 1).over(w4).alias("prev_age"),
    )
)

result = (
    with_windows
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
    )
)
