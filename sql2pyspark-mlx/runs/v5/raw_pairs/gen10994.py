from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country", "city").orderBy(F.col("age").desc(), "id")
w3 = Window.partitionBy("segment", "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("credit_limit", "id")

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull())
    .withColumn("email_country", F.concat("email", F.lit(" "), "country"))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("DIRECT")))
)

result = (
    cleaned
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w)), 4))
    .withColumn("age_bucket", F.ntile(3).over(w2))
    .withColumn("running_credit_limit", F.sum("credit_limit").over(w3))
    .withColumn("prev_credit_limit", F.lag("credit_limit", 1).over(w4))
)
