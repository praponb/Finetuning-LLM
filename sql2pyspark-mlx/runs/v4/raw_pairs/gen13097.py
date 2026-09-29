from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")

valid = (
    df_products
    .filter(F.col("price").between(610, 970))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.trim("product_name").alias("clean_product_name"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.supplier_name"))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched_products = (
    deduped
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.rating"),
        F.col("d.city"),
        F.col("d.supplier_name").alias("supplier_supplier_name"),
    )
)

result = (
    enriched_products
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "tags",
        "supplier_name",
        "rating",
        F.when(F.col("brand").rlike("^(Umbrella|Stark)$"), "PRIORITY").when(F.col("brand").rlike("^(Acme|Globex)$"), "GROUP_B").otherwise("GROUP_A").alias("brand_zone"),
    )
    .orderBy(F.col("category").asc_nulls_first())
)
