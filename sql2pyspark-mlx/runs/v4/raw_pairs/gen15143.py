from pyspark.sql import functions as F

result = (
    df_products
    .select("cost", "supplier_id", F.array_join("tags", ", ").alias("tags_text"))
    .orderBy("tags_text")
)
