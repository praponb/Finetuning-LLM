from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"), F.col("c.price"))
)

result = (
    with_last
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "product_stock_qty",
        F.when(F.col("country").rlike("^(US|JP)$"), "CORE").when(F.col("country").rlike("^(TH|SG)$"), "GROUP_A").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("id").desc())
)
