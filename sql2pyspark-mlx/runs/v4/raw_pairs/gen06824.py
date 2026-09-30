from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc(), "id")

latest = (
    df_products
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "tags",
        "supplier_id",
        "rank_in_group",
        F.when(F.col("product_name").rlike("^(Desk Lamp|USB-C Hub)$"), "GROUP_A").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("product_name_zone"),
    )
    .orderBy(F.col("supplier_id").desc_nulls_first())
)
