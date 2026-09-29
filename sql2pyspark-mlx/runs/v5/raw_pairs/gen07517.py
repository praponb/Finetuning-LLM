from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("launch_date") > F.lit("2023-05-18").cast("date")))
    .withColumn("price_int", F.col("price").cast("int"))
)

result = (
    valid
    .select("id", "supplier_id", "cost", "price")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["cost", "price"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)
