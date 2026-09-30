from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value").orderBy(F.col("num_values").desc())

flattened = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("value"))

result = (
    flattened
    .groupBy("value", "customer_id")
    .agg(
        F.sort_array(F.collect_set("value")).alias("value_list"),
        F.count(F.when(F.col("value") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.size(F.collect_set("value")).alias("num_values"),
    )
    .filter(F.col("welcome5_rows") < 1000)
    .withColumn("num_values_rank", F.rank().over(w))
    .orderBy("value")
)
