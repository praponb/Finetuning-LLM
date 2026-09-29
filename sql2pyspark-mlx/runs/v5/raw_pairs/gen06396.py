from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "customer_id", "shipping_fee", "discount", "amount")
    .unpivot(
        ids=["id", "customer_id"],
        values=["shipping_fee", "discount", "amount"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
