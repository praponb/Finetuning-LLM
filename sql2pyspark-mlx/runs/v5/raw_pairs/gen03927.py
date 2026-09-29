from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "category", "price", "cost")
    .unpivot(
        ids=["id", "category"],
        values=["price", "cost"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
