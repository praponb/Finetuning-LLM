from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age", "id").rowsBetween(-5, -1)
w2 = Window.orderBy("age", "id")

metrics = (
    df_customers
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.avg("credit_limit").over(w).alias("credit_limit_avg_5_prior"),
        F.lag("age", 2).over(w2).alias("prev_age"),
    )
)

result = (
    metrics
    .select(
        "id",
        "name",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "credit_limit_avg_5_prior",
    )
)
