from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

deduped = (
    df_products
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(Running Shoes|Desk Lamp)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("product_name_zone"),
        F.col("min_product_cost"),
    )
    .orderBy(F.col("product_name_zone").desc_nulls_first())
)
