from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        F.col("price").alias("PRICE"),
        F.col("cost").alias("COST"),
    )
    .unpivot(["id", "supplier_id"], ["PRICE", "COST"], "kpi", "val")
    .filter(F.col("val").isNotNull())
)
