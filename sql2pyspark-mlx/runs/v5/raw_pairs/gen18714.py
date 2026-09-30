from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")

deduped = (
    df_suppliers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "city",
        "rating",
        F.when(F.col("city").rlike("^(Chiang Mai|Berlin)$"), "CORE").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc())
)
