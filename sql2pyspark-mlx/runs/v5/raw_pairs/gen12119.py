from pyspark.sql import functions as F

valid = (
    df_products
    .filter(~F.col("brand").eqNullSafe(F.lit("Stark")))
    .withColumn("brand_length", F.length("brand"))
)

result = (
    valid
    .select("id", "supplier_id", "price", "cost")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["price", "cost"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
