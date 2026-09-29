from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("credit_limit").isNull())
    .select(
        "id",
        "segment",
        "signup_date",
        "age",
        F.sum("age").over(w).alias("running_age"),
    )
    .orderBy("signup_date", "running_age")
)
