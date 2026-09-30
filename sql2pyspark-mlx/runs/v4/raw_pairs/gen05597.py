from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("signup_date", "id")
w_recent = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")
w3 = Window.partitionBy("city", "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("segment", "city").orderBy(F.col("age").desc_nulls_first())
w5 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .withColumn("next_age", F.lead("age", 2).over(w_running))
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("first_known_segment", F.first("segment", ignorenulls=True).over(w3))
    .withColumn("age_cume_dist", F.cume_dist().over(w4))
    .withColumn("cumulative_age", F.sum("age").over(w5))
)
