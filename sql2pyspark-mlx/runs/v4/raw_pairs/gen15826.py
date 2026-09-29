from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        F.col("cost").alias("COST_VAL"),
        F.col("price").alias("PRICE_VAL"),
    )
    .unpivot(["id", "supplier_id"], ["COST_VAL", "PRICE_VAL"], "metric", "val")
    .filter(F.col("val").isNotNull())
)
