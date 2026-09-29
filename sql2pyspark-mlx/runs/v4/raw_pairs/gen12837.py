from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("city").orderBy("rating", "id")
w_running = Window.partitionBy("city")
w3 = Window.partitionBy("country")
w_ordered = Window.orderBy("rating", "id")
w5 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-5, -1)

filtered = (
    df_suppliers
    .filter(F.col("rating") >= 2.4)
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 4))
    .withColumn("city_clean", F.coalesce("city", F.lit("DIRECT")))
)

result = (
    filtered
    .withColumn("event_seq", F.row_number().over(w_recent))
    .withColumn("city_min_rating", F.min("rating").over(w_running))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w3)), 4))
    .withColumn("next_rating", F.lead("rating", 1, 0).over(w_ordered))
    .withColumn("rating_avg_5_prior", F.avg("rating").over(w5))
)
