from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
    )
)
