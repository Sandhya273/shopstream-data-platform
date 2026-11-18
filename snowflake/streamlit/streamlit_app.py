"""
Streamlit in Snowflake app (optional BI surface inside Snowflake).
Deploy: Snowsight > Projects > Streamlit > + Streamlit App, warehouse BI_WH, role ANALYST_ROLE.
"""
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()
st.title("ShopStream - Daily Sales")

days = st.slider("Days", 7, 180, 30)
channel = st.multiselect("Channel", ["web", "app", "marketplace"], default=["web", "app", "marketplace"])
ch = ",".join(f"'{c}'" for c in channel) or "''"

df = session.sql(f"""
    select order_date, sum(net_revenue) net_revenue, sum(orders) orders
    from analytics_db.marts.mart_daily_sales
    where order_date >= dateadd(day, -{int(days)}, (select max(order_date) from analytics_db.marts.mart_daily_sales))
      and channel in ({ch})
    group by 1 order by 1
""").to_pandas()

c1, c2, c3 = st.columns(3)
c1.metric("Net revenue", f"₹{df.NET_REVENUE.sum() / 1e5:,.1f} L")
c2.metric("Orders", f"{int(df.ORDERS.sum()):,}")
c3.metric("AOV", f"₹{df.NET_REVENUE.sum() / max(df.ORDERS.sum(), 1):,.0f}")
st.line_chart(df, x="ORDER_DATE", y="NET_REVENUE")

recon = session.sql("""
    select recon_status, count(*) orders, sum(capture_variance) variance
    from analytics_db.marts.mart_payment_reconciliation group by 1 order by 2 desc
""").to_pandas()
st.subheader("Payment reconciliation")
st.dataframe(recon, use_container_width=True)
