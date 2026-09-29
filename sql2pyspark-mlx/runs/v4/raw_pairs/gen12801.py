from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w_hist = Window.partitionBy("country").orderBy(F.col("credit_limit").asc_nulls_last())
w3 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w))
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("first_known_age", F.first("age", ignorenulls=True).over(w3))
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w4))
)
