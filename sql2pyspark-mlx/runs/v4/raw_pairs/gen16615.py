from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter(F.col("email").isNotNull())
    .withColumn("name_title", F.initcap(F.trim("name")))
    .withColumn("clean_name", F.trim("name"))
)

result = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date").alias("order_order_date"))
)
