from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "country",
        "signup_date",
        "age",
        "city",
        F.sum("age").over(w).alias("running_age"),
        F.sum("age").over(w2).alias("running_age_2"),
    )
)
