from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "credit_limit",
        "age",
        F.ntile(3).over(w).alias("age_bucket"),
        F.first("age", ignorenulls=True).over(w2).alias("first_known_age"),
    )
)
