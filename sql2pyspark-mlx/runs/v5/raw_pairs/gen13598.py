from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_customers
    .select(
        "*",
        F.last("segment", ignorenulls=True).over(w).alias("last_known_segment"),
        F.sum("age").over(w_grp).alias("cumulative_age"),
    )
)

result = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
    )
    .orderBy(F.col("credit_limit").desc())
)
