from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country").orderBy("age", "id")
w3 = Window.partitionBy("country").orderBy("credit_limit", "id")
w4 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("age", "id").rowsBetween(-6, -1)

with_windows = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .select(
        "id",
        "email",
        "segment",
        "age",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.lead("age", 1).over(w2).alias("next_age"),
        F.lag("age", 1).over(w3).alias("prev_age"),
        F.sum("credit_limit").over(w4).alias("cumulative_credit_limit"),
        F.avg("credit_limit").over(w5).alias("credit_limit_avg_6_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "email",
        "age",
        "age_share",
        "prev_age",
        "cumulative_credit_limit",
        "credit_limit_avg_6_prior",
    )
    .orderBy(F.col("cumulative_credit_limit").desc())
)
