from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 45000)
    .select("opened_date")
    .subtract(
        df_shipments
        .filter(F.month("delivered_date").isin([1, 7, 9]))
        .select("delivered_date")
    )
)
