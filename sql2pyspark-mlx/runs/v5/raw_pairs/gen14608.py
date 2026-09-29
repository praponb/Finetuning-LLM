from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
        F.sum("age").over(w2).alias("running_age"),
    )
)
