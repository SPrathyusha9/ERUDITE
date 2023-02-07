import streamlit as st
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import streamlit_extras
from streamlit_extras.switch_page_button import switch_page
import datetime
import numpy as np
from pandas.api.types import CategoricalDtype
 

def email_segments():
    st.write("")
    st.write("<center><h2 style='color:#330066'>Email Segmentation</h2></center>", unsafe_allow_html=True)
    # code to display email segments data
    
    Host=st.text_input("Host")
    Port=st.text_input("Port")
    Database=st.text_input("Database")
    User=st.text_input("User")
    Password=st.text_input("Password", type="password")
    if not all([Host, Port, Database, User, Password]):
        st.error("Please enter all fields.\n If this error is showing even after entering all the fields, Please contact the winning team")
    else:
        try:
            if "conn" not in st.session_state:
                st.session_state.conn = init_get_connection(Host,Port,Database,User,Password)
                
            sent_table = st.text_input("Enter Sent table as Schema_name.Sent_Table")
            open_table = st.text_input("Enter Open table(Schema_name.Open_Table)")
            click_table = st.text_input("Enter Clicks table (Schema_name.Click_Table)")
            bounce_table = st.text_input("Enter Bounce table (Schema_name.Bounce_Table)")
            unsubscribe_table = st.text_input("Enter Unsubscribe table (Schema_name.Unsubscribe_Table)")
            sendjobs_table = st.text_input("Enter Sendjobs table (Schema_name.Sendjobs_Table)")

            if not all([ sent_table, open_table, click_table, unsubscribe_table, sendjobs_table]):
                st.error("Please enter all fields")
            else:   
                    if "cursor" not in st.session_state:
                        st.session_state.cursor = st.session_state.conn.cursor()
                        st.write("2")
                    
                    if "sql_query1" not in st.session_state:
                        st.session_state.sql_query1 = '''drop table if exists temp.email_engagement_erudite'''
                        st.session_state.cursor=st.session_state.conn.cursor()
                        st.write("3")
                        st.session_state.cursor.execute(st.session_state.sql_query1)
                        st.session_state.sql_query1 = '''
                        create table temp.email_engagement_erudite as
                        (select
                        b0.sendid,
                        b0.emailname,
                        b0.subject,
                        b0.campaign_senttime,
                        b0.subscriberkey, b0.subscriberid,
                        case when b0.sendid = g0.sendid and b0.subscriberid = g0.subscriberid then 1 else 0 end as isbounced,
                        bouncetype,
                        g0.bounce_timestamp,
                        c0.total_opens,
                        c0.first_open_timestamp, c0.last_open_timestamp,
                        d0.total_clicks, d0.first_click_timestamp, d0.last_click_timestamp,
                        case when c0.first_open_timestamp is null then null else datediff(hour, b0.senttime, c0.first_open_timestamp) end as time_to_first_open,
                        h0.unsubscribed_timestamp,
                        case when 
                            b0.sendid = h0.sendid 
                            and 
                            b0.subscriberid = h0.subscriberid 
                            then 1
                            else 0 
                            end as has_unsubscribed 
                        from 
                        (
                        select a1.subscriberkey
                        , a1.subscriberid
                        , a1.sendid
                        , b1.subject
                        , b1.emailname
                        , cast(substring(a1.eventdate,1,10) as date) as campaign_senttime
                        , eventdate as senttime
                        from '''+sent_table+'''  a1 
                            left join 
                            (
                                select sendid,
                                subject,
                                emailname 
                                from '''+sendjobs_table +''') b1 
                                on a1.sendid = b1.sendid
                            where campaign_senttime <= '2023-01-31') b0
                            left join 
                            (
                                select subscriberkey,
                                subscriberid,
                                sendid,
                                count(*) as total_opens,
                                min(eventdate) as first_open_timestamp,
                                max(eventdate) as last_open_timestamp 
                                from '''+open_table+''' where eventdate <= '2023-01-31'
                            group by subscriberkey, subscriberid, sendid) c0 
                            ON b0.subscriberid = c0.subscriberid and b0.sendid = c0.sendid
                            left join 
                            (
                                select
                                subscriberkey,
                                subscriberid, 
                                sendid,
                                count(*) as total_clicks,
                                min(eventdate) as first_click_timestamp,
                                max(eventdate) as last_click_timestamp 
                                from '''+click_table+''' where eventdate <= '2023-01-31'
                                group by subscriberkey, subscriberid, sendid) d0 
                                ON b0.subscriberid = d0.subscriberid and b0.sendid = d0.sendid
                                left join (select subscriberkey
                                , subscriberid
                                , sendid
                                , min(eventdate) as bounce_timestamp
                                , bouncecategory as bouncetype
                                , max(bouncereason) as bouncereason 
                                from '''+bounce_table+''' where eventdate <= '2023-01-31'
                                group by 
                                subscriberkey, 
                                subscriberid, 
                                sendid,
                                bouncecategory) g0
                                ON b0.subscriberid = g0.subscriberid and b0.sendid = g0.sendid
                                left join 
                                (select subscriberkey, subscriberid, sendid, min(eventdate) as unsubscribed_timestamp
                                from '''+unsubscribe_table+''' where eventdate <= '2023-01-31'
                                group by subscriberkey, subscriberid, sendid) h0
                                ON b0.subscriberid = h0.subscriberid and b0.sendid = h0.sendid)
                            '''
                        st.session_state.cursor.execute(st.session_state.sql_query1)
                        st.write("4")
                    #Works till here 
                    if "sql_query2" not in st.session_state:
                        st.session_state.sql_query2 = '''drop table if exists temp.email_summary_master'''
                        st.session_state.cursor.execute(st.session_state.sql_query2)
                        st.write("5")
                        st.session_state.sql_query2 = '''
                        create table temp.email_summary_master as 
                        select a.*
                        from (SELECT sendid, emailname, subject, campaign_senttime as send_date,
                        count(case when campaign_senttime is not null then subscriberkey end) as ttl_sent_mails,
                        count(case when campaign_senttime is not null and isbounced=0 then subscriberkey end) as ttl_delivered,
                        count(case when campaign_senttime is not null and isbounced=1 then subscriberkey end) as ttl_bounced,
                        count(distinct case when total_opens>0 then subscriberkey end) as unique_opens,
                        count(distinct case when total_clicks>0 then subscriberkey end) as unique_clicks,
                        count(distinct case when has_unsubscribed=1 then subscriberkey end) as ttl_unsubscribes
                        from temp.email_engagement_erudite
                        group by sendid, emailname, subject, send_date) a'''

                        st.session_state.cursor.execute(st.session_state.sql_query2)

                        st.write(6)    

                    if "sql_query3" not in st.session_state:
                        st.session_state.sql_query3 = "SELECT * FROM temp.email_summary_master"

                    if "df1" not in st.session_state: 
                        st.session_state.df1 = pd.read_sql(st.session_state.sql_query3, st.session_state.conn)

                    sql_query=''' GRANT ALL ON temp.email_summary_master to public '''
                    st.session_state.cursor.execute(sql_query) 
                    st.write(7)       

                    email_summary(st.session_state.df1)
                    st.write(8)
                    email_trend(st.session_state.df1)
                    st.write(9)
                    email_findings(st.session_state.df1)  
                    st.write(10)   

                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("") 
                    st.write("") 
                    st.write("") 
                    st.write("") 
                    st.write("<h2 style='color:grey'>Open Rate vs # of Customers</h2>", unsafe_allow_html=True)
                    st.write("") 
                    st.write("") 
                    x=st.radio(
                        "Select a Time Period 👇",
                        ["90 days", "180 days", "365 days"],
                        label_visibility="visible",
                        horizontal=True
                    )

                    st.write("")
                    st.write("") 
                    st.write("") 
                    sql_query = "drop table if exists temp.open_rate_master"
                    st.session_state.cursor.execute(sql_query)    
                    st.write(11)
                    sql_query = '''
                        create table temp.open_rate_master as
                        (select a.*
                        from 
                        (SELECT subscriberkey --DATE_TRUNC('month', campaign_senttime) as month,
                        ,datediff(day,min(campaign_senttime),'2023-01-31') as days_on_file
                        ,count(case when campaign_senttime is not null and 
                            (campaign_senttime between dateadd('day',-180,'2023-01-31') and '2023-01-31') 
                            then sendid end) as ttl_sent_mails_180days,
                        count(case when campaign_senttime is not null and isbounced=0
                            and (campaign_senttime between dateadd('day',-180,'2023-01-31') and '2023-01-31')
                            then sendid end) as ttl_delivered_180days,
                        count(distinct case when total_opens>0 and (campaign_senttime between dateadd('day',-180,'2023-01-31') and '2023-01-31') 
                            then sendid end) as unique_opens_180days,
                        count(distinct case when total_clicks>0 and (campaign_senttime between dateadd('day',-180,'2023-01-31') and '2023-01-31')
                            then sendid end) as unique_clicks_180days,
                        count(case when campaign_senttime is not null and 
                            (campaign_senttime between dateadd('day',-90,'2023-01-31') and '2023-01-31') 
                            then sendid end) as ttl_sent_mails_90days,
                        count(case when campaign_senttime is not null and isbounced=0
                            and (campaign_senttime between dateadd('day',-90,'2023-01-31') and '2023-01-31')
                            then sendid end) as ttl_delivered_90days,
                        count(distinct case when total_opens>0 and (campaign_senttime between dateadd('day',-90,'2023-01-31') and '2023-01-31') 
                            then sendid end) as unique_opens_90days,
                        count(distinct case when total_clicks>0 and (campaign_senttime between dateadd('day',-90,'2023-01-31') and '2023-01-31')
                            then sendid end) as unique_clicks_90days,
                        count(case when campaign_senttime is not null and 
                            (campaign_senttime between dateadd('day',-365,'2023-01-31') and '2023-01-31') 
                            then sendid end) as ttl_sent_mails_365days,
                        count(case when campaign_senttime is not null and isbounced=0
                            and (campaign_senttime between dateadd('day',-365,'2023-01-31') and '2023-01-31')
                            then sendid end) as ttl_delivered_365days,
                        count(distinct case when total_opens>0 and (campaign_senttime between dateadd('day',-365,'2023-01-31') and '2023-01-31') 
                            then sendid end) as unique_opens_365days,
                        count(distinct case when total_clicks>0 and (campaign_senttime between dateadd('day',-365,'2023-01-31') and '2023-01-31')
                            then sendid end) as unique_clicks_365days
                        from 
                        (select a.*, case when campaign_senttime > b.unsubscribed_date then 0 else 1 end as unsubscribe_exclusion
                        from temp.email_engagement_erudite a
                        left join 
                        (select subscriberkey,max(cast(unsubscribed_timestamp as date)) as unsubscribed_date 
                        from temp.email_engagement_erudite
                        group by subscriberkey) b 
                        on a.subscriberkey = b.subscriberkey
                        )
                        where unsubscribe_exclusion = 1
                        group by subscriberkey) a)
                    '''
                    st.session_state.cursor.execute(sql_query)

                    sql_query = "GRANT ALL ON temp.open_rate_master TO public"
                    st.session_state.cursor.execute(sql_query)

                    st.write(12)
                    if x=="180 days":

                        sql_query = '''SELECT CASE WHEN ttl_delivered_180days=0 THEN -1 ELSE ROUND((CAST(unique_opens_180days*100 AS float))/CAST(ttl_delivered_180days AS float),2) END AS open_rate_180days
                        ,ttl_delivered_180days,count(DISTINCT subscriberkey) AS no_of_cust_180days
                        FROM temp.open_rate_master
                        WHERE open_rate_180days <=100 and (ttl_delivered_180days>=3 or open_rate_180days=-1)
                        GROUP BY 1,2'''
                        df_open_180 = pd.read_sql(sql_query, st.session_state.conn)
                        

                        threshold,threshold1=Open_rate_cust_180(df_open_180)

                        sql_query = '''SELECT Segment
                                    ,count(DISTINCT subscriberkey) as no_of_cust
                                    ,ROUND(CAST((SUM(unique_opens_180days)*100) AS float)/CAST(SUM(ttl_delivered_180days) AS float),2) AS avg_open_rate
                                    ,ROUND(CAST((SUM(unique_clicks_180days)*100) AS float)/CAST(SUM(ttl_delivered_180days) AS float),2) AS avg_click_rate
                                    FROM 
                                    (SELECT CASE WHEN ttl_delivered_180days=0 THEN -1 ELSE ROUND((CAST(unique_opens_180days*100 AS float))/CAST(ttl_delivered_180days AS float),2) END AS open_rate_180days
                                        ,CASE 
                                        WHEN open_rate_180days=-1 THEN 'No Emails Delivered'
                                        WHEN ttl_delivered_180days<3 THEN 'New Customer'
                                        WHEN open_rate_180days=0 THEN 'No Emails Opened'
                                        WHEN open_rate_180days>0 AND open_rate_180days<='''+str(threshold)+''' THEN 'Low'
                                        WHEN open_rate_180days>'''+str(threshold)+''' AND open_rate_180days<='''+str(threshold1)+''' THEN 'Medium'
                                        WHEN open_rate_180days>'''+str(threshold1)+''' AND open_rate_180days<=100 THEN 'High' 
                                        ELSE 'Other' END AS Segment
                                        ,*
                                        FROM temp.open_rate_master)
                                        where Segment!='No Emails Delivered' 
                                        GROUP BY 1
                                '''

                        df_engagement = pd.read_sql(sql_query, st.session_state.conn)

                        engagement_summary(df_engagement)
                    
                    elif x=="90 days":
                        sql_query = '''SELECT CASE WHEN ttl_delivered_90days=0 THEN -1 ELSE ROUND((CAST(unique_opens_90days*100 AS float))/CAST(ttl_delivered_90days AS float),2) END AS open_rate_90days
                        ,count(DISTINCT subscriberkey) AS no_of_cust_90days
                        FROM temp.open_rate_master
                        WHERE open_rate_90days <=100 and (ttl_delivered_90days>=3 or open_rate_90days=-1)
                        GROUP BY 1'''
                        df_open_90 = pd.read_sql(sql_query, st.session_state.conn)
                        st.write(13)

                        threshold,threshold1=Open_rate_cust_90(df_open_90)

                        sql_query = '''SELECT Segment
                                    ,count(DISTINCT subscriberkey) as no_of_cust
                                    ,ROUND(CAST((SUM(unique_opens_90days)*100) AS float)/CAST(SUM(ttl_delivered_90days) AS float),2) AS avg_open_rate
                                    ,ROUND(CAST((SUM(unique_clicks_90days)*100) AS float)/CAST(SUM(ttl_delivered_90days) AS float),2) AS avg_click_rate
                                    FROM 
                                    (SELECT CASE WHEN ttl_delivered_90days=0 THEN -1 ELSE ROUND((CAST(unique_opens_90days*100 AS float))/CAST(ttl_delivered_90days AS float),2) END AS open_rate_90days
                                        ,CASE 
                                        WHEN open_rate_90days=-1 THEN 'No Emails Delivered'
                                        WHEN ttl_delivered_90days<3 or days_on_file<30 THEN 'New Customer'
                                        WHEN open_rate_90days=0 THEN 'No Emails Opened'
                                        WHEN open_rate_90days>0 AND open_rate_90days<='''+str(threshold)+''' THEN 'Low'
                                        WHEN open_rate_90days>'''+str(threshold)+''' AND open_rate_90days<='''+str(threshold1)+''' THEN 'Medium'
                                        WHEN open_rate_90days>'''+str(threshold1)+''' AND open_rate_90days<=100 THEN 'High' 
                                        ELSE 'Other' END AS Segment
                                        ,*
                                        FROM temp.open_rate_master)
                                        where Segment!='No Emails Delivered' 
                                        GROUP BY 1
                                '''

                        df_engagement = pd.read_sql(sql_query, st.session_state.conn)
                        st.write(14)
                        engagement_summary(df_engagement)
                    
                    elif x=="365 days":
                        sql_query = '''SELECT CASE WHEN ttl_delivered_365days=0 THEN -1 ELSE ROUND((CAST(unique_opens_365days*100 AS float))/CAST(ttl_delivered_365days AS float),2) END AS open_rate_365days
                        ,count(DISTINCT subscriberkey) AS no_of_cust_365days
                        FROM temp.open_rate_master
                        WHERE open_rate_365days <=100 and (ttl_delivered_365days>=3 or open_rate_365days=-1)
                        GROUP BY 1'''
                        df_open_365 = pd.read_sql(sql_query, st.session_state.conn)
                        

                        threshold,threshold1=Open_rate_cust_365(df_open_365)

                        sql_query = '''SELECT Segment
                                    ,count(DISTINCT subscriberkey) as no_of_cust
                                    ,ROUND(CAST((SUM(unique_opens_365days)*100) AS float)/CAST(SUM(ttl_delivered_365days) AS float),2) AS avg_open_rate
                                    ,ROUND(CAST((SUM(unique_clicks_365days)*100) AS float)/CAST(SUM(ttl_delivered_365days) AS float),2) AS avg_click_rate
                                    FROM 
                                    (SELECT CASE WHEN ttl_delivered_365days=0 THEN -1 ELSE ROUND((CAST(unique_opens_365days*100 AS float))/CAST(ttl_delivered_365days AS float),2) END AS open_rate_365days
                                        ,CASE 
                                        WHEN open_rate_365days=-1 THEN 'No Emails Delivered'
                                        WHEN ttl_delivered_365days<3 THEN 'New Customer'
                                        WHEN open_rate_365days=0 THEN 'No Emails Opened'
                                        WHEN open_rate_365days>0 AND open_rate_365days<='''+str(threshold)+''' THEN 'Low'
                                        WHEN open_rate_365days>'''+str(threshold)+''' AND open_rate_365days<='''+str(threshold1)+''' THEN 'Medium'
                                        WHEN open_rate_365days>'''+str(threshold1)+''' AND open_rate_365days<=100 THEN 'High' 
                                        ELSE 'Other' END AS Segment
                                        ,*
                                        FROM temp.open_rate_master)
                                        where Segment!='No Emails Delivered'
                                        GROUP BY 1
                                '''

                        df_engagement = pd.read_sql(sql_query, st.session_state.conn)

                        engagement_summary(df_engagement)
                    
                    sql_query = "DROP TABLE IF EXISTS TEMP.time_to_open_master"
                    st.session_state.cursor.execute(sql_query)
                    st.write(15)

                    sql_query = '''
                        CREATE TABLE TEMP.time_to_open_master AS 
                    (select subscriberkey,
                            count(sendid) as ttl_sent_mails,
                            count(case when isbounced=0 then sendid end) as ttl_delivered,
                            count(distinct case when total_opens>0 then sendid end) as unique_opens,
                            count(distinct case when total_clicks>0 then sendid end) as unique_clicks,
                            datediff(day,min(first_sent_date),'2023-01-31') as days_on_file,
                            datediff(day,max(last_sent_date),'2023-01-31') as days_since_last_sent,
                            datediff(day,max(last_open_date),'2023-01-31') as days_since_last_open,
                            datediff(day,max(last_click_date),'2023-01-31') as days_since_last_click,
                            max(campaign_senttime) as last_email_send_date,
                            max(cast(last_open_timestamp as date)) as last_email_open_date,
                            datediff('day',last_email_open_date,last_email_send_date) as diff_btw_last_send_last_open
                        from 
                            (select a.*, case when campaign_senttime > b.unsubscribed_date then 0 else 1 end as unsubscribe_exclusion,
                                first_sent_date,last_sent_date,last_open_date,last_click_date
                                from temp.email_engagement_erudite a
                                left join 
                                (select subscriberkey,max(cast(unsubscribed_timestamp as date)) as unsubscribed_date, min(campaign_senttime) as first_sent_date,
                                        max(campaign_senttime) as last_sent_date,max(cast(last_open_timestamp as date)) as last_open_date,
                                        max(cast(last_click_timestamp as date)) as last_click_date
                                from temp.email_engagement_erudite
                                group by subscriberkey) b 
                                on a.subscriberkey = b.subscriberkey
                                )
                        where unsubscribe_exclusion = 1
                        group by subscriberkey)
                    '''
                    st.session_state.cursor.execute(sql_query)
                    
                    st.write(16)

                    sql_query = "grant all on TEMP.time_to_open_master to public"
                    st.session_state.cursor.execute(sql_query)
                    
                    st.write(17)

                    sql_query='''SELECT CASE WHEN diff_btw_last_send_last_open<0 then 0 else diff_btw_last_send_last_open end as diff_btw_last_send_last_open ,count(DISTINCT subscriberkey) as no_of_cust
                    FROM TEMP.time_to_open_master
                    WHERE ttl_delivered != 0 AND unique_opens !=0 and ttl_delivered>=3
                    GROUP BY 1
                    ORDER BY 1 asc '''
                    
                    df_segments = pd.read_sql(sql_query, st.session_state.conn)
                    
                    st.write(17)

                    time_to_last_open(df_segments)
                    
                    st.write(18)

                    sql_query='''
                    SELECT CASE WHEN ttl_delivered=0 THEN 'No Emails Delivered'
					WHEN ttl_delivered<3 OR days_on_file<30 THEN 'New Customer' 
                    WHEN days_since_last_sent >=365 THEN 'Not Targeted'
                    WHEN days_since_last_open IS NULL AND COALESCE(days_since_last_open,0) >=365 AND days_since_last_sent <=365 THEN 'Never Opened'
                    WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)<=45 THEN 'Engaged'
                    WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>45 AND COALESCE(days_since_last_open,0)<=90 THEN 'Engaged but at Risk'
                    WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>90 AND COALESCE(days_since_last_open,0)<=180 THEN 'Inactive'
                    WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>180 AND COALESCE(days_since_last_open,0)<365 THEN 'Dormant'
                    ELSE 'Other' END AS Segment
                    ,count(DISTINCT subscriberkey) as no_of_cust
                    ,sum(ttl_delivered) AS Total_Sent
                    ,sum(unique_opens) AS Total_Open
                    ,sum(unique_clicks) AS Total_Clicks
                    FROM TEMP.time_to_open_master
                    GROUP BY 1
                    '''

                    df_send_open = pd.read_sql(sql_query, st.session_state.conn)
                    segments(df_send_open)


                    if x=="90 days":

                        threshold = st.slider("Please Re -Enter the Low-Medium Open Rate Threshold", min_value=0.0, max_value=100.0, value=50.0, step=0.01)
                        threshold1 = st.slider("Please Re -Enter the Medium-High Open Rate Threshold", min_value=0.0, max_value=100.0, value=80.0, step=0.01)
                        if threshold>=threshold1:
                            st.write("Please enter appropriate values")
                        else:
                            sql_query = '''DROP TABLE IF EXISTS temp.erudite_combined_segments'''
                            st.session_state.cursor.execute(sql_query)

                            sql_query = '''CREATE TABLE temp.erudite_combined_segments AS 
                            (SELECT segment,segment2,a.subscriberkey
                            FROM
                            (SELECT CASE WHEN ttl_delivered_90days=0 THEN -1 ELSE ROUND((CAST(unique_opens_90days*100 AS float))/CAST(ttl_delivered_90days AS float),2) END AS open_rate_90days
                            ,CASE 
                            WHEN open_rate_90days=-1 THEN 'No Emails Delivered'
                            WHEN ttl_delivered_90days<3 or days_on_file<30 THEN 'New Customer'
                            WHEN open_rate_90days=0 THEN 'No Emails Opened'
                            WHEN open_rate_90days>0 AND open_rate_90days<='''+str(threshold)+''' THEN 'Low'
                            WHEN open_rate_90days>'''+str(threshold)+''' AND open_rate_90days<='''+str(threshold1)+''' THEN 'Medium'
                            WHEN open_rate_90days>'''+str(threshold1)+''' AND open_rate_90days<=100 THEN 'High' 
                            ELSE 'Other' END AS Segment
                            ,*
                            FROM temp.open_rate_master)a
                            LEFT JOIN   (SELECT CASE WHEN ttl_delivered=0 THEN 'No Emails Delivered'
                                            WHEN ttl_delivered<3 OR days_on_file<30 THEN 'New Customer' 
                                            WHEN days_since_last_sent >=365 THEN 'Not Targeted'
                                            WHEN days_since_last_open IS NULL AND COALESCE(days_since_last_open,0) >=365 AND days_since_last_sent <=365 THEN 'Never Opened'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)<=45 THEN 'Engaged'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>45 AND COALESCE(days_since_last_open,0)<=90 THEN 'Engaged but at Risk'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>90 AND COALESCE(days_since_last_open,0)<=180 THEN 'Inactive'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>180 AND COALESCE(days_since_last_open,0)<365 THEN 'Dormant'
                                            ELSE 'Other' END AS Segment2
                                            ,*
                                            FROM TEMP.time_to_open_master )b
                                ON a.subscriberkey =b.subscriberkey) '''


                            st.session_state.cursor.execute(sql_query)

                            sql_query = '''SELECT * 
                                FROM temp.erudite_combined_segments 
                                PIVOT (
                                    COUNT(DISTINCT subscriberkey) FOR Segment2 IN ('No Emails Delivered','New Customer' ,'Not Targeted','Never Opened','Engaged','Engaged but at Risk','Inactive','Dormant','Other'))
                            '''

                            df = pd.read_sql(sql_query,st.session_state.conn)
                            segments_engagement_summary(df)

                    elif x=="180 days": 
                        threshold = st.slider("Please Re -Enter the Low-Medium Open Rate Threshold", min_value=0.0, max_value=100.0, value=50.0, step=0.01)
                        threshold1 = st.slider("Please Re -Enter the Medium-High Open Rate Threshold", min_value=0.0, max_value=100.0, value=80.0, step=0.01)
                        if threshold>=threshold1:
                            st.write("Please enter appropriate values")
                        else:
                            sql_query = '''DROP TABLE IF EXISTS temp.erudite_combined_segments'''
                            st.session_state.cursor.execute(sql_query)

                            sql_query = '''CREATE TABLE temp.erudite_combined_segments AS 
                            (SELECT segment,segment2,a.subscriberkey
                            FROM
                            (SELECT CASE WHEN ttl_delivered_180days=0 THEN -1 ELSE ROUND((CAST(unique_opens_180days*100 AS float))/CAST(ttl_delivered_180days AS float),2) END AS open_rate_180days
                            ,CASE 
                            WHEN open_rate_180days=-1 THEN 'No Emails Delivered'
                            WHEN ttl_delivered_180days<3 or days_on_file<30 THEN 'New Customer'
                            WHEN open_rate_180days=0 THEN 'No Emails Opened'
                            WHEN open_rate_180days>0 AND open_rate_180days<='''+str(threshold)+''' THEN 'Low'
                            WHEN open_rate_180days>'''+str(threshold)+''' AND open_rate_180days<='''+str(threshold1)+''' THEN 'Medium'
                            WHEN open_rate_180days>'''+str(threshold1)+''' AND open_rate_180days<=100 THEN 'High' 
                            ELSE 'Other' END AS Segment
                            ,*
                            FROM temp.open_rate_master)a
                            LEFT JOIN   (SELECT CASE WHEN ttl_delivered=0 THEN 'No Emails Delivered'
                                            WHEN ttl_delivered<3 OR days_on_file<30 THEN 'New Customer' 
                                            WHEN days_since_last_sent >=365 THEN 'Not Targeted'
                                            WHEN days_since_last_open IS NULL AND COALESCE(days_since_last_open,0) >=365 AND days_since_last_sent <=365 THEN 'Never Opened'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)<=45 THEN 'Engaged'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>45 AND COALESCE(days_since_last_open,0)<=90 THEN 'Engaged but at Risk'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>90 AND COALESCE(days_since_last_open,0)<=180 THEN 'Inactive'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>180 AND COALESCE(days_since_last_open,0)<365 THEN 'Dormant'
                                            ELSE 'Other' END AS Segment2
                                            ,*
                                            FROM TEMP.time_to_open_master )b
                                ON a.subscriberkey =b.subscriberkey) '''

                            st.session_state.cursor(sql_query)

                            sql_query = '''SELECT * 
                                FROM temp.erudite_combined_segments 
                                PIVOT (
                                    COUNT(DISTINCT subscriberkey) FOR Segment2 IN ('No Emails Delivered','New Customer' ,'Not Targeted','Never Opened','Engaged','Engaged but at Risk','Inactive','Dormant','Other'))
                            '''

                            df = pd.read_sql(sql_query,st.session_state.conn)
                            segments_engagement_summary(df)

                    elif x== "365 days":
                        threshold = st.slider("Please Re -Enter the Low-Medium Open Rate Threshold", min_value=0.0, max_value=100.0, value=50.0, step=0.01)
                        threshold1 = st.slider("Please Re -Enter the Medium-High Open Rate Threshold", min_value=0.0, max_value=100.0, value=80.0, step=0.01)
                        if threshold>=threshold1:
                            st.write("Please enter appropriate values")
                        else:
                            sql_query = '''DROP TABLE IF EXISTS temp.erudite_combined_segments'''
                            st.session_state.cursor.execute(sql_query)

                            sql_query = '''CREATE TABLE temp.erudite_combined_segments AS 
                            (SELECT segment,segment2,a.subscriberkey
                            FROM
                            (SELECT CASE WHEN ttl_delivered_365days=0 THEN -1 ELSE ROUND((CAST(unique_opens_365days*100 AS float))/CAST(ttl_delivered_365days AS float),2) END AS open_rate_365days
                            ,CASE 
                            WHEN open_rate_365days=-1 THEN 'No Emails Delivered'
                            WHEN ttl_delivered_365days<3 or days_on_file<30 THEN 'New Customer'
                            WHEN open_rate_365days=0 THEN 'No Emails Opened'
                            WHEN open_rate_365days>0 AND open_rate_365days<='''+str(threshold)+''' THEN 'Low'
                            WHEN open_rate_365days>'''+str(threshold)+''' AND open_rate_365days<='''+str(threshold1)+''' THEN 'Medium'
                            WHEN open_rate_365days>'''+str(threshold1)+''' AND open_rate_365days<=100 THEN 'High' 
                            ELSE 'Other' END AS Segment
                            ,*
                            FROM temp.open_rate_master)a
                            LEFT JOIN   (SELECT CASE WHEN ttl_delivered=0 THEN 'No Emails Delivered'
                                            WHEN ttl_delivered<3 OR days_on_file<30 THEN 'New Customer' 
                                            WHEN days_since_last_sent >=365 THEN 'Not Targeted'
                                            WHEN days_since_last_open IS NULL AND COALESCE(days_since_last_open,0) >=365 AND days_since_last_sent <=365 THEN 'Never Opened'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)<=45 THEN 'Engaged'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>45 AND COALESCE(days_since_last_open,0)<=90 THEN 'Engaged but at Risk'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>90 AND COALESCE(days_since_last_open,0)<=180 THEN 'Inactive'
                                            WHEN days_since_last_sent IS NOT NULL AND COALESCE(days_since_last_open,0)>180 AND COALESCE(days_since_last_open,0)<365 THEN 'Dormant'
                                            ELSE 'Other' END AS Segment2
                                            ,*
                                            FROM TEMP.time_to_open_master )b
                                ON a.subscriberkey =b.subscriberkey) '''
                            

                            st.session_state.cursor.execute(sql_query)

                            sql_query = '''SELECT * 
                                FROM temp.erudite_combined_segments 
                                PIVOT (
                                    COUNT(DISTINCT subscriberkey) FOR Segment2 IN ('No Emails Delivered','New Customer' ,'Not Targeted','Never Opened','Engaged','Engaged but at Risk','Inactive','Dormant','Other'))
                            '''

                            df = pd.read_sql(sql_query,st.session_state.conn)
                            segments_engagement_summary(df) 

                    #segments_engagement_summary(df)
                    # Close the connection and st.session_state.st.session_state.cursor

                    st.button('Close DB Connection', on_click=close_conn)

        except Exception as err:
            print ("Oops! An exception has occured:", err)
            print ("Exception TYPE:", type(err))
            st.write(type(err))

def close_conn():
    st.session_state.cursor.close()
    st.session_state.conn.close() 

def email_summary(data):
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("<h2 style='color:grey'>Email Summary</h2>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        Start_Date = st.date_input("Start Date", datetime.date(2022, 2, 1),key='Start Date 1')
    with col2:
        End_Date = st.date_input("End Date", datetime.date(2023, 1, 31),key='End Date 1')
    st.write("") # Add a blank line to separate the input from the results
    data = data[data["send_date"]>=Start_Date]
    data = data[data["send_date"]<=End_Date]

    total_sent = data["ttl_sent_mails"].sum()
    total_delivered = data["ttl_delivered"].sum()
    total_bounces = data["ttl_bounced"].sum()
    total_clicks = data["unique_clicks"].sum()
    total_opens = data["unique_opens"].sum()
    total_unsubscribed = data["ttl_unsubscribes"].sum()
    open_rate = total_opens / total_delivered
    click_rate = total_clicks / total_delivered

    st.write("Total Sent: ", total_sent)
    st.write("Total Delivered: ", total_delivered)
    st.write("Total Bounces: ", total_bounces)
    st.write("Total Clicks: ", total_clicks)
    st.write("Total Opens: ", total_opens)
    st.write("Total Unsubscribed: ", total_unsubscribed)
    st.write("Open Rate: ", (open_rate*100).round(2),"%")
    st.write("Click Rate: ", (click_rate*100).round(2),"%") 

def email_trend(data):
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("<h2 style='color:grey'>Email Trends</h2>", unsafe_allow_html=True)
    st.write("")
    col1, col2 = st.columns(2)

    with col1:
        Start_Date= st.date_input("Start Date", datetime.date(2022, 2, 1),key='Start Date 2')
    with col2:
        End_Date = st.date_input("End Date", datetime.date(2023, 1, 31),key = 'End Date 2')
    st.write("") # Add a blank line to separate the input from the results
    data = data[data["send_date"]>=Start_Date]
    data = data[data["send_date"]<=End_Date]
    data = data.groupby('send_date').sum()
    data["Open Rate"] = (data["unique_opens"]/data["ttl_delivered"])*100
    data["Click Rate"] = (data["unique_clicks"]/data["ttl_delivered"])*100
    data["Delivery Rate"] = (data["ttl_delivered"]/data["ttl_sent_mails"])*100
    data = data.reset_index()
    data = data[["send_date","Delivery Rate","Open Rate","Click Rate"]]
    data = data.groupby('send_date').sum()
    st.line_chart(data)

def email_findings(data):
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("<h2 style='color:grey'>Email Performance</h2>", unsafe_allow_html=True)
    st.write("")
    col1, col2 = st.columns(2)
    with col1:
        Start_Date= st.date_input("Start Date", datetime.date(2022, 2, 1),key='Start Date 3')
    
    with col2:
        End_Date = st.date_input("End Date", datetime.date(2023, 1, 31),key = 'End Date 3')
    st.write("") # Add a blank line to separate the input from the results
    data = data[data["send_date"]>=Start_Date]
    data = data[data["send_date"]<=End_Date]

    data = data.groupby(['emailname']).sum()
    data["Open Rate"] = (data["unique_opens"]/data["ttl_delivered"])*100
    data["Click Rate"] = (data["unique_clicks"]/data["ttl_delivered"])*100
    data["Delivery Rate"] = (data["ttl_delivered"]/data["ttl_sent_mails"])*100
    data["Unsubscribe Rate"] = (data["ttl_unsubscribes"]/data["ttl_delivered"])*100
    data = data.reset_index()
    
    metric = ['Delivered','Opened', 'Clicked', 'Unsubscribed']
    selected_option = st.selectbox("Select a Metric", metric)

    if selected_option=="Delivered":
        # sort the dataframe by delivery_rate in descending order
        data = data.sort_values(by='Delivery Rate', ascending=False)
        col1, col2 = st.columns(2)
        
        with col1:
            # select the top 5 rows
            top_5 = data[['emailname','Delivery Rate']].head(5)
            top_5 = top_5.reset_index(drop=True)
            # display the top 5 emails
            st.write("Top 5 emails based on "+selected_option+" Rate:")

            st.dataframe(top_5)
        with col2: 
            # select the bottom 5 rows
            bottom_5 = data[['emailname','Delivery Rate']].tail(5).sort_values(by='Delivery Rate', ascending=True)
            bottom_5 = bottom_5.reset_index(drop=True)
            # display the bottom 5 emails
            st.write("Bottom 5 emails based on "+selected_option+" Rate:")

            st.dataframe(bottom_5)

    elif selected_option=="Opened":
        # sort the dataframe by delivery_rate in descending order
        data = data.sort_values(by='Open Rate', ascending=False)

        col1, col2 = st.columns(2)
        
        with col1:
            # select the top 5 rows
            top_5 = data[['emailname','Open Rate']].head(5)
            top_5 = top_5.reset_index(drop=True)
            # display the top 5 emails
            st.write("Top 5 emails based on "+selected_option+" Rate:")

            st.dataframe(top_5)
        with col2: 
            # select the bottom 5 rows
            bottom_5 = data[['emailname','Open Rate']].tail(5).sort_values(by='Open Rate', ascending=True)
            bottom_5 = bottom_5.reset_index(drop=True)
            # display the bottom 5 emails
            st.write("Bottom 5 emails based on "+selected_option+" Rate:")
            
            st.dataframe(bottom_5)
        
    elif selected_option=="Clicked":
        # sort the dataframe by delivery_rate in descending order
        data = data.sort_values(by='Click Rate', ascending=False)

        col1, col2 = st.columns(2)
        
        with col1:
            # select the top 5 rows
            top_5 = data[['emailname','Click Rate']].head(5)
            top_5 = top_5.reset_index(drop=True)
            # display the top 5 emails
            st.write("Top 5 emails based on "+selected_option+" Rate:")

            st.dataframe(top_5)
        with col2: 
            # select the bottom 5 rows
            bottom_5 = data[['emailname','Click Rate']].tail(5).sort_values(by='Click Rate', ascending=True)
            bottom_5 = bottom_5.reset_index(drop=True)
            # display the bottom 5 emails
            st.write("Bottom 5 emails based on "+selected_option+" Rate:")
            
            st.dataframe(bottom_5)
    
    elif selected_option=="Unsubscribed":
        # sort the dataframe by delivery_rate in descending order
        data = data.sort_values(by='Unsubscribe Rate', ascending=False)

        col1, col2 = st.columns(2)
        
        with col1:
            # select the top 5 rows
            top_5 = data[['emailname','Unsubscribe Rate']].head(5)
            top_5 = top_5.reset_index(drop=True)
            # display the top 5 emails
            st.write("Top 5 emails based on "+selected_option+" Rate:")

            st.dataframe(top_5)
        with col2: 
            # select the bottom 5 rows
            bottom_5 = data[['emailname','Unsubscribe Rate']].tail(5).sort_values(by='Unsubscribe Rate', ascending=True)
            bottom_5 = bottom_5.reset_index(drop=True)
            # display the bottom 5 emails
            st.write("Bottom 5 emails based on "+selected_option+" Rate:")
            
            st.dataframe(bottom_5)

def Open_rate_cust_180(data):
    data = data[['open_rate_180days','no_of_cust_180days']]
    data = data.groupby('open_rate_180days').sum()

    data = data.reset_index()

    x=int(data[data['open_rate_180days']==-1]['no_of_cust_180days'])
    y = int(data[data['open_rate_180days']==0]['no_of_cust_180days'])
    st.write("Number of Customers with no delivered emails: ",x)
    st.write("Number of Customers who did not open any emails: ",y)
    st.write("") 
    st.write("") 
    data = data[data['open_rate_180days']!=-1]
    data = data[data['open_rate_180days']!=0]
    
    data = data.sort_values(by='open_rate_180days')
    st.dataframe(data)
    data['cumulative'] = (data['no_of_cust_180days'].cumsum()/data['no_of_cust_180days'].sum())*100

    fig, ax = plt.subplots()
    plt.plot(data['open_rate_180days'], data['cumulative'])
    plt.ylabel("% of Customers")
    plt.xlabel("Open Rate")

    # Add the threshold
    threshold = st.slider("Low-Medium Open Rate Threshold", min_value=0.0, max_value=100.0, value=50.0, step=0.01)
    threshold2 = st.slider("Medium-High Open Rate Threshold", min_value=0.0, max_value=100.0, value=80.0, step=0.01)
    if(threshold>=threshold2):
        st.error("Please enter appropriate thresholds")
    else:
        t1 = int(data.iloc[(data['open_rate_180days']-threshold).abs().argsort()[:1]]['cumulative'])
        t2 = int(data.iloc[(data['open_rate_180days']-threshold2).abs().argsort()[:1]]['cumulative'])

        plt.axhline(y=t1, color='red', linestyle='--')
        plt.axhline(y=t2, color='purple', linestyle='--')

        st.pyplot(fig)

    return threshold,threshold2
        
def Open_rate_cust_90(data):
    data = data[['open_rate_90days','no_of_cust_90days']]
    data = data.groupby('open_rate_90days').sum()

    data = data.reset_index()

    x=int(data[data['open_rate_90days']==-1]['no_of_cust_90days'])
    y = int(data[data['open_rate_90days']==0]['no_of_cust_90days'])
    st.write("Number of Customers with no delivered emails: ",x)
    st.write("Number of Customers who did not open any emails: ",y)
    st.write("") 
    st.write("") 
    data = data[data['open_rate_90days']!=-1]
    data = data[data['open_rate_90days']!=0]
    
    data = data.sort_values(by='open_rate_90days')
    st.dataframe(data)
    data['cumulative'] = (data['no_of_cust_90days'].cumsum()/data['no_of_cust_90days'].sum())*100

    fig, ax = plt.subplots()
    plt.plot(data['open_rate_90days'], data['cumulative'])
    plt.ylabel("% of Customers")
    plt.xlabel("Open Rate")

    # Add the threshold
    threshold = st.slider("Low-Medium Open Rate Threshold", min_value=0.0, max_value=100.0, value=50.0, step=0.01)
    threshold2 = st.slider("Medium-High Open Rate Threshold", min_value=0.0, max_value=100.0, value=80.0, step=0.01)
    if(threshold>=threshold2):
        st.error("Please enter appropriate thresholds")
    else:
        t1 = int(data.iloc[(data['open_rate_90days']-threshold).abs().argsort()[:1]]['cumulative'])
        t2 = int(data.iloc[(data['open_rate_90days']-threshold2).abs().argsort()[:1]]['cumulative'])

        plt.axhline(y=t1, color='red', linestyle='--')
        plt.axhline(y=t2, color='purple', linestyle='--')

        st.pyplot(fig)

    return threshold,threshold2

def Open_rate_cust_365(data):
    data = data[['open_rate_365days','no_of_cust_365days']]
    data = data.groupby('open_rate_365days').sum()

    data = data.reset_index()
    data = data.sort_values(by='open_rate_365days')
    st.dataframe(data)
    st.dataframe(data[data['open_rate_365days']==0]['no_of_cust_365days'])
    x=int(data[data['open_rate_365days']==-1]['no_of_cust_365days'])
    y = int(data[data['open_rate_365days']==0]['no_of_cust_365days'])
    st.write("Number of Customers with no delivered emails: ",x)
    st.write("Number of Customers who did not open any emails: ",y)
    st.write("") 
    st.write("") 
    data = data[data['open_rate_365days']!=-1]
    data = data[data['open_rate_365days']!=0]
    
    data = data.sort_values(by='open_rate_365days')
    data['cumulative'] = (data['no_of_cust_365days'].cumsum()/data['no_of_cust_365days'].sum())*100

    fig, ax = plt.subplots()
    plt.plot(data['open_rate_365days'], data['cumulative'])
    plt.ylabel("% of Customers")
    plt.xlabel("Open Rate")

    # Add the threshold
    threshold = st.slider("Low-Medium Open Rate Threshold", min_value=0.0, max_value=100.0, value=50.0, step=0.01)
    threshold2 = st.slider("Medium-High Open Rate Threshold", min_value=0.0, max_value=100.0, value=80.0, step=0.01)
    if(threshold>=threshold2):
        st.error("Please enter appropriate thresholds")
    else:
        t1 = int(data.iloc[(data['open_rate_365days']-threshold).abs().argsort()[:1]]['cumulative'])
        t2 = int(data.iloc[(data['open_rate_365days']-threshold2).abs().argsort()[:1]]['cumulative'])

        plt.axhline(y=t1, color='red', linestyle='--')
        plt.axhline(y=t2, color='purple', linestyle='--')

        st.pyplot(fig)

    return threshold,threshold2

def engagement_summary(data):
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("<h2 style='color:grey'>Engagement Summary</h2>", unsafe_allow_html=True)
    st.write("")
    data.rename(columns = {'segment':'Segment','no_of_cust':'# of Customers','avg_open_rate':'Average Open Rate','avg_click_rate':'Average Click Rate'},inplace = True)
    
    # CSS to inject contained in a string
    hide_table_row_index = """
                <style>
                thead tr th:first-child {display:none}
                tbody th {display:none}
                </style>
                """
    data = data[data['Segment']!='No Emails Delivered']
    data = data[data['Segment']!='Other']
    segments = CategoricalDtype(['Low', 'Medium', 'High','New Customer','No Emails Opened'], ordered=True)
    data['Segment'] = data['Segment'].astype(segments)
    st.markdown(hide_table_row_index, unsafe_allow_html=True)
    st.table(data.sort_values(by='Segment'))

def time_to_last_open(data):
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("<h2 style='color:grey'>Time to Last Open</h2>", unsafe_allow_html=True)
    st.write("")
    
    data['cumulative'] = (data['no_of_cust'].cumsum()/data['no_of_cust'].sum())*100
    fig, ax = plt.subplots()
    #data['diff_btw_last_send_last_open'].max()
    threshold3 = st.slider("Low-Medium Days to Last Open Threshold", min_value=0.0, max_value=float(data['diff_btw_last_send_last_open'].max()), value=50.0, step=0.01)
    threshold4 = st.slider("Medium-High Days to Last Open Threshold", min_value=0.0, max_value=float(data['diff_btw_last_send_last_open'].max()), value=80.0, step=0.01)
    if(threshold3>=threshold4):
        st.error("Please enter appropriate thresholds")
    else:
        t1 = int(data.iloc[(data['diff_btw_last_send_last_open']-threshold3).abs().argsort()[:1]]['cumulative'])
        t2 = int(data.iloc[(data['diff_btw_last_send_last_open']-threshold4).abs().argsort()[:1]]['cumulative'])

        plt.plot(data['diff_btw_last_send_last_open'], data['cumulative'])
        plt.ylabel("% of Customers")
        plt.xlabel("Days between Last Sent and Opened")
        plt.axhline(y=t1, color='red', linestyle='--')
        plt.axhline(y=t2, color='purple', linestyle='--')

        
        st.pyplot(fig)

def segments(data):
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("<h2 style='color:grey'>Segments</h2>", unsafe_allow_html=True)
    st.write("")
    data.rename(columns = {'segment':'Segment','no_of_cust':'# of Customers','total_sent':'Total Sent','total_open':'Total Opens','total_clicks':'Total Clicks'},inplace = True)
    # CSS to inject contained in a string
    hide_table_row_index = """
                <style>
                thead tr th:first-child {display:none}
                tbody th {display:none}
                </style>
                """

    #segments = CategoricalDtype(['Low', 'Medium', 'High'], ordered=True)
    #data['Segment'] = data['Segment'].astype(segments)
    st.markdown(hide_table_row_index, unsafe_allow_html=True)
    #st.table(data.sort_values(by='Segment'))
    st.table(data)


def segments_engagement_summary(data):
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("<h2 style='color:grey'>Engagement Segments</h2>", unsafe_allow_html=True)
    st.write("")
    st.write(data)

@st.experimental_singleton
def init_get_connection(Host,Port,Database,User,Password):
    return psycopg2.connect(
        host=Host,
        port=Port,
        database=Database,
        user=User,
        password=Password
        )

def ltv_segments():
    st.write("<center><h2 style='color:#330066'>LTV Segments</h2></center>", unsafe_allow_html=True)
    # code to display LTV segments data
    Host=st.text_input("Host")
    Port=st.text_input("Port")
    Database=st.text_input("Database")
    User=st.text_input("User")
    Password=st.text_input("Password", type="password") 
    
    try:
        conn = init_get_connection(Host,Port,Database,User,Password)
        

        Transaction_Table = st.text_input("Enter Transcation table as Schema_name.Transcation_Table")
        cust_id = st.text_input("Enter column name for customer id")
        transaction_date = st.text_input("Enter column name for Transaction Date")
        transaction_amt = st.text_input("Enter column name for Transaction Amount")
        if not all([ Transaction_Table, cust_id, transaction_date, transaction_amt]):
            st.error("Please enter all fields")
        else:     
            st.session_state.cursor = conn.cursor()
            sql_query = '''drop table if exists temp.revenue_master_erudite'''

            st.session_state.cursor.execute(sql_query)

            sql_query='''create table temp.revenue_master_erudite as
                (select loyaltymemberkey ,p12m_txns,p12m_netsales,p6m_txns,p6m_netsales,
                case when p12m_txns > 0 then p12m_netsales*1.00/p12m_txns else 0 end as avg_rev_12m
                from 
                (
                select loyaltymemberkey,
                sum(p12m) as p12m_txns,
                sum(p6m) as p6m_txns,
                sum(case when p6m = 1 then netsalesamount else 0 end) as p6m_netsales,
                sum(case when p12m = 1 then netsalesamount else 0 end) as p12m_netsales,
                max(case when p12m = 1 then checkdate end) as last_trans
                from
                (
                select '''+cust_id+''' as loyaltymemberkey,'''+transaction_date+''' as checkdate,'''+transaction_amt+''' as netsalesamount,
                case when checkdate between dateadd(day,-365,'2023-01-31') and '2023-01-31' then 1 else 0 end as p12m,
                case when checkdate between dateadd(day,-180,'2023-01-31') and '2023-01-31' then 1 else 0 end as p6m
                from '''+Transaction_Table+''' 
                )
                where p12m = 1 or '''+cust_id+''' in (select distinct '''+cust_id+''' from '''+Transaction_Table+''')
                group by loyaltymemberkey) )'''

            st.session_state.cursor.execute(sql_query)  

            sql_query = ''' SELECT * FROM temp.revenue_master_erudite'''

            df = pd.read_sql(sql_query,conn) 
            freq_dist(df)
            cum_freq_dist(df)
            revenue_dist(df)

            sql_query = '''drop table if exists temp.recency_master'''
            st.session_state.cursor.execute(sql_query)  

            sql_query = '''create table temp.recency_master as
            (select '''+cust_id+''',datediff('day',last_txn_date,'2023-01-31') as days_since_last_purchase, 
            ttl_gap_days*1.00/(case when ttl_gap_days > 0 then cnt_gaps when ttl_gap_days = 0 then 1 end) as avg_days_between_purchase,
            case when (ttl_txns = 1 or ttl_txn_days = 1) and (last_txn_date between dateadd(day,-180,'2023-01-31') and '2023-01-31') then 'Active'
                when (ttl_txns = 1 or ttl_txn_days = 1) and last_txn_date < dateadd(day,-180,'2023-01-31') then 'Delayed'
                when ttl_txns = 0 then 'Delayed'
                else 'TBD' end as recency_segment
            from
            (select '''+cust_id+''',count('''+transaction_date+''') as ttl_txns,
            max('''+transaction_date+''') as last_txn_date,
            sum(coalesce(days_since_previous_purchase,0)) as ttl_gap_days,
            sum(coalesce(prev_txn_flag,0)) as cnt_gaps,
            count(distinct '''+transaction_date+''') as ttl_txn_days
            from
            (select '''+cust_id+''','''+transaction_date+''','''+transaction_amt+''',
            LAG('''+transaction_date+''') OVER (partition by '''+cust_id+''' ORDER BY '''+transaction_date+''') as prev_txn,
            datediff('day',prev_txn,'''+transaction_date+''') as days_since_previous_purchase,
            case when prev_txn is not null then 1 else 0 end as prev_txn_flag
            from
            (
            select '''+cust_id+''','''+transaction_date+''','''+transaction_amt+''',
            case when '''+transaction_date+''' between dateadd(day,-365,'2023-01-31') and '2023-01-31' then 1 else 0 end as p12m,
            case when '''+transaction_date+''' between dateadd(day,-180,'2023-01-31') and '2023-01-31' then 1 else 0 end as p6m
            from '''+Transaction_Table+''' 
            )
            )
            group by '''+cust_id+'''))'''

            st.session_state.cursor.execute(sql_query)

            sql_query = '''drop table if exists temp.recency_segment'''

            st.session_state.cursor.execute(sql_query)

            sql_query ='''create table temp.recency_segment as
                select *, 
                case when avg_days_between_purchase = 0 then recency_segment
                    when recency_segment = 'TBD' and (days_since_last_purchase*1.0000/avg_days_between_purchase between 0.0000 and 1.2500) then 'Active'
                    when recency_segment = 'TBD' and (days_since_last_purchase*1.0000/avg_days_between_purchase > 1.2500 and days_since_last_purchase*1.0000/avg_days_between_purchase <= 2.0000) then 'Active_At_Risk'
                    when recency_segment = 'TBD' and days_since_last_purchase*1.0000/avg_days_between_purchase > 2.0000 then 'Delayed'
                    when recency_segment <> 'TBD' then recency_segment 
                    end as segment_recency
                    ,case when avg_days_between_purchase = 0 then null
                       when recency_segment = 'TBD' then days_since_last_purchase*1.0000/avg_days_between_purchase end as index
                from temp.recency_master'''
            
            st.session_state.cursor.execute(sql_query)

            sql_query = '''select * from temp.recency_segment'''

            df = pd.read_sql(sql_query,conn)
            Recency(df)

    except Exception as err:
        print ("Oops! An exception has occured:", err)
        print ("Exception TYPE:", type(err))

def Recency(data):
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("<h2 style='color:grey'>Recency</h2>", unsafe_allow_html=True)

    data = data[data['recency_segment'] == "TBD"]
    data = pd.DataFrame(data.groupby(['index'])['loyaltymemberkey'].count().reset_index())
    data['cumulative'] = (data['loyaltymemberkey'].cumsum()/data['loyaltymemberkey'].sum())*100
    
    fig, ax = plt.subplots()
    plt.plot(data['index'].round(0),data['cumulative'])
    plt.xticks(np.arange(0, 10, step=0.5))
    #plt.ylim(bottom = 0,top = 100)
    plt.xlim(left = 0,right=10)
    plt.ylabel("% of Customers")
    plt.xlabel("index")

    st.pyplot(fig)

def freq_dist(data):
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("<h2 style='color:grey'>Frequency Distribution</h2>", unsafe_allow_html=True)

    data = data[data['p12m_txns']!=0]

    data = data.sort_values(by='p12m_txns')

    df1 = pd.DataFrame(data.groupby(['p12m_txns'])['loyaltymemberkey'].count().reset_index())

    fig, ax = plt.subplots()
    plt.plot(df1['p12m_txns'], df1['loyaltymemberkey'])
    plt.xticks(np.arange(0, 15, step=1)) 
    plt.xlim(left = 0,right=15)
    plt.ylabel("Count of Customers")
    plt.xlabel("Frequency")

    st.pyplot(fig)

def cum_freq_dist(data):
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("<h2 style='color:grey'>Cumulative Frequency Distribution</h2>", unsafe_allow_html=True)

    data = data[data['p12m_txns']!=0]
    data = data.sort_values(by='p12m_txns')

    df2 = pd.DataFrame(data.groupby(['p12m_txns'])['loyaltymemberkey'].count().reset_index())
    df2['cumulative'] = (df2['loyaltymemberkey'].cumsum()/df2['loyaltymemberkey'].sum())*100
    
    fig, ax = plt.subplots()
    plt.plot(df2['p12m_txns'], df2['cumulative'])
    #plt.xticks(np.arange(0, 15, step=1)) 
    #plt.xlim(left = 0,right=15)
    plt.ylabel("% of Customers")
    plt.xlabel("Frequency")

    col1, col2 = st.columns(2)

    with col1:
        threshold = st.slider("Low-Medium Frequency Threshold", min_value=0.0, max_value=max(df2['p12m_txns']), value=3, step=0.01)
    with col2:
        threshold2 = st.slider("Medium-High Frequency Threshold", min_value=0.0,max_value=max(df2['p12m_txns']), value=10.0, step=0.01)

    if(threshold>=threshold2):
        st.error("Please enter appropriate thresholds")
    else:
        t1 = int(df2.iloc[(df2['avg_rev_12m']-threshold).abs().argsort()[:1]]['cumulative'])
        t2 = int(df2.iloc[(df2['avg_rev_12m']-threshold2).abs().argsort()[:1]]['cumulative'])

        plt.axhline(y=t1, color='red', linestyle='--')
        plt.axhline(y=t2, color='purple', linestyle='--')

    st.pyplot(fig)

def revenue_dist(data):
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("") 
    st.write("<h2 style='color:grey'>Revenue Distribution</h2>", unsafe_allow_html=True)

    data['avg_rev_12m'] = data['avg_rev_12m'].round(0)
    data = data[data['avg_rev_12m'] >= 0]

    df3 = pd.DataFrame(data.groupby(['avg_rev_12m'])['loyaltymemberkey'].count().reset_index())
    df3['cumulative'] = (df3['loyaltymemberkey'].cumsum()/df3['loyaltymemberkey'].sum())*100

    fig, ax = plt.subplots()
    plt.plot(df3['cumulative'],df3['avg_rev_12m'])
    plt.xticks(np.arange(0, 95, step=10)) 
    plt.ylim(bottom = 0,top = 100)
    #plt.xlim(left = 0,right=100)
    plt.ylabel("Avg Revenue($)")
    plt.xlabel("% of Customers")
    col1, col2 = st.columns(2)

    with col1:
        threshold = st.slider("Low-Medium Revenue Threshold", min_value=0.0, max_value=max(df3['avg_rev_12m']), value=15.0, step=0.01)
    with col2:
        threshold2 = st.slider("Medium-High Revenue Threshold", min_value=0.0,max_value=max(df3['avg_rev_12m']), value=30.0, step=0.01)

    if(threshold>=threshold2):
        st.error("Please enter appropriate thresholds")
    else:
        t1 = int(df3.iloc[(df3['avg_rev_12m']-threshold).abs().argsort()[:1]]['cumulative'])
        t2 = int(df3.iloc[(df3['avg_rev_12m']-threshold2).abs().argsort()[:1]]['cumulative'])

        plt.axvline(x=t1, color='red', linestyle='--')
        plt.axvline(x=t2, color='purple', linestyle='--')
    

    st.pyplot(fig)

def main():
    st.set_page_config(page_title='Segmentation_Suite')
    st.title("Segmentation Suite")
    st.sidebar.title("Select Page")
    page = st.sidebar.selectbox("", ["Email Segments", "LTV Segments", "Email+LTV Segments"],label_visibility="hidden")

    if page == "Email Segments":
        email_segments()
    elif page == "LTV Segments":
        ltv_segments()

if __name__ == "__main__":
    main()

