from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "discount",
        "amount",
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["discount", "amount", "SHIPPING_FEE_VAL"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
