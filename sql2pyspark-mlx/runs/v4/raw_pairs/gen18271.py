from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("supplier_id").isNull())
    .withColumn("category_brand_key", F.concat_ws("-", "category", "brand"))
    .withColumn("is_umbrella", F.when(F.col("brand") == "Umbrella", 1).otherwise(0))
)

result = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)
