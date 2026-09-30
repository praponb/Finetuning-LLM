from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.rating", F.col("c.brand"), F.col("c.cost").alias("product_cost"))
)

result = (
    with_latest
    .select(
        "id",
        "brand",
        "product_cost",
        F.when(F.col("brand").rlike("^(Globex|Umbrella)$"), "OTHER_ZONE").when(F.col("brand").rlike("^(Acme|Initech)$"), "PRIORITY").otherwise("GROUP_B").alias("brand_zone"),
    )
    .orderBy(F.col("brand").desc())
)
