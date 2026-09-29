from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "customer_id", "amount", "discount", "shipping_fee")
    .unpivot(
        ids=["id", "customer_id"],
        values=["amount", "discount", "shipping_fee"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
