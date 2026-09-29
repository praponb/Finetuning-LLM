from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating_int").asc_nulls_first())
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")
w_part = Window.partitionBy("city").orderBy("rating_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "country").orderBy("rating_int", "id")
w6 = Window.partitionBy("city")

cleaned = (
    df_suppliers
    .filter(F.length("supplier_name") > 7)
    .withColumn("rating_int", F.col("rating").cast("int"))
    .withColumn("city_clean", F.coalesce("city", F.lit("OTHER")))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost") < 280), "left_semi")
)

deduped = (
    eligible
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .withColumn("cumulative_rating", F.sum("rating").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("cumulative_rating_int", F.sum("rating_int").over(w_part))
    .withColumn("prev_rating", F.lag("rating", 2).over(w5))
    .withColumn("city_max_rating_int", F.max("rating_int").over(w6))
)
