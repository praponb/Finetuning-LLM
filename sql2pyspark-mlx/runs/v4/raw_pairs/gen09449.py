from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(
        F.countDistinct("cost").alias("unique_suppliers"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .orderBy("category", "brand")
    .limit(50)
)
