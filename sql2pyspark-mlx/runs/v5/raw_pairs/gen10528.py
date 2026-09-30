from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .filter(F.col("city").isin(["London", "Berlin"]))
    .select(
        "id",
        "city",
        "age",
        "signup_date",
        F.avg("age").over(w).alias("city_avg_age"),
        F.sum("age").over(w2).alias("running_age"),
    )
)
