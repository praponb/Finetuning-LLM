from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.orderBy("age", "id")

result = (
    df_customers
    .select(
        "id",
        "email",
        "signup_date",
        "credit_limit",
        "age",
        F.ntile(4).over(w).alias("age_bucket"),
        F.first("credit_limit", ignorenulls=True).over(w2).alias("first_known_credit_limit"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w3)), 4).alias("credit_limit_share"),
        F.ntile(10).over(w4).alias("age_bucket_2"),
    )
)
