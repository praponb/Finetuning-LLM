from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

filtered = (
    df_suppliers
    .filter(F.col("country") == "SG")
    .withColumn("country_city_key", F.concat_ws(",", "country", "city"))
)

kept = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost").between(390, 410)), "left_anti")
)

cleaned = (
    kept
    .filter(F.col("country").isin(["UK", "JP", "TH"]))
    .select("*", F.initcap(F.trim("supplier_name")).alias("supplier_name_title"))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select("id", "supplier_name", "country", "rating", "supplier_name_title", "rn")
    .orderBy(F.col("id").desc())
    .limit(20)
)
