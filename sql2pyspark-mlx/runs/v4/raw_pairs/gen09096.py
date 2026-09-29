from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.month("sale_date").isin([1, 5, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.round("revenue", 1).alias("revenue_rounded"),
        F.round("revenue", 1).alias("revenue_rounded_2"),
    )
)

result = (
    filtered
    .select("id", "product_id", "revenue_rounded_2", "revenue", "revenue_rounded")
    .unpivot(
        ["id", "product_id"],
        ["revenue_rounded_2", "revenue", "revenue_rounded"],
        "measure",
        "value",
    )
)
