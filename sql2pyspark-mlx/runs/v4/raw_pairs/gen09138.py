from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "supplier_id", "cost", "price")
    .unpivot(["id", "supplier_id"], ["cost", "price"], "metric", "reading")
    .filter(F.col("reading").isNotNull())
)
