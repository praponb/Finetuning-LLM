from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_last(), "id")
w2 = Window.partitionBy("city", "country").orderBy(F.col("latest_rank").desc(), "id")

base = (
    df_suppliers
    .filter(~F.col("country").isin(["SG", "US", "DE"]))
    .withColumn("city_supplier_name_key", F.concat_ws("/", "city", "supplier_name"))
)

eligible = (
    base
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.product_name").like("Yog%")), "left_semi")
)

ranked = (
    eligible
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select("id", "country", "rating", "city_supplier_name_key", "latest_rank", "rn")
    .orderBy(F.col("id").asc_nulls_first())
    .limit(20)
)
