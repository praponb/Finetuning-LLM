from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "channel", "amount", "discount", "shipping_fee")
    .unpivot(
        ids=["id", "channel"],
        values=["amount", "discount", "shipping_fee"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
