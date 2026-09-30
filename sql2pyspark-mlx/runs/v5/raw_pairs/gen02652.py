from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "category",
        F.col("price").alias("PRICE"),
        F.col("cost").alias("COST"),
    )
    .unpivot(
        ids=["id", "category"],
        values=["PRICE", "COST"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
