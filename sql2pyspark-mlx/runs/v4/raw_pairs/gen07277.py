from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("country", "city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("country", "city").orderBy("age", "id")

result = (
    df_customers
    .withColumn("last_known_age", F.last("age", ignorenulls=True).over(w_ordered))
    .withColumn("age_avg_3", F.avg("age").over(w2))
    .withColumn("age_bucket", F.ntile(3).over(w_grp))
)
