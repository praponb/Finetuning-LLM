from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .select(
        "id",
        "email",
        "city",
        "signup_date",
        "is_active",
        "age",
        F.max("credit_limit").over(w).alias("last3_max_credit_limit"),
        F.avg("credit_limit").over(w).alias("last3_avg_credit_limit"),
        F.min("credit_limit").over(w).alias("last3_min_credit_limit"),
        F.last("credit_limit", ignorenulls=True).over(w2).alias("last_known_credit_limit"),
    )
)
