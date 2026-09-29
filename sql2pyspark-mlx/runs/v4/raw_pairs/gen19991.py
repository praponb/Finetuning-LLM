from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc_nulls_first())
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, -1)

metrics = (
    df_suppliers
    .withColumn("rating_percent_rank", F.percent_rank().over(w))
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w2))
    .withColumn("rating_avg_3_prior", F.avg("rating").over(w3))
)

result = (
    metrics
    .select(
        "id",
        "country",
        "city",
        "rating",
        "rating_avg_3_prior",
        F.when(F.col("city").rlike("^(Tokyo|Singapore)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("city").desc(), "id")
)
