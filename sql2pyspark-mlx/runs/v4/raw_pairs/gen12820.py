from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("discount").desc(), "id")

valid = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-04-10"))) & F.col("customer_id").isNotNull() & F.col("shipping_fee").isNotNull())
    .withColumn("is_web", F.when(F.col("channel") == "web", 1).otherwise(0))
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
