with
    INDUSTRY_TABLE as (
        select distinct
            CU_COMPANY_ID,
            CU_COMPANY_L1_INDUSTRY INDUSTRY
        from
            CDM.COUNTERPARTY.CARDUP_COMPANY_T
        where
            COMPANY_CU_LOCALE_ID = 1
    ),
    COLLECT_AGGREGATED_ALL as (
        select
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID as COMPANY_ID,
            case
                when CARDUP_PAYMENT_SALES_OWNER is null then 'Unmanaged'
                else 'Managed'
            end as SALES_OWNERSHIP,
            DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) as MONTH_TRANSACTED,
            DENSE_RANK() OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
                order by
                    DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) DESC
            ) as LATEST_MONTH_RANK,
            COUNT(distinct DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS))) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) as MONTH_ACTIVE_COUNT_COLLECT,
            count(distinct DWH_CARDUP_PAYMENT_ID) as TOTAL_TX_COUNT_COLLECT,
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
            inner join INDUSTRY_TABLE T2 on T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = T2.CU_COMPANY_ID
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
            and CARDUP_PAYMENT_CU_LOCALE_ID = 1
            and LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%collect%'
            --and CARDUP_PAYMENT_SALES_OWNER is not null
            and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) <= DATE('2025-01-01')
        group by
            1,
            2,
            3
    ),
    MAIN_AGGREGATED_ALL as (
        select
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID as COMPANY_ID,
            case
                when CARDUP_PAYMENT_SALES_OWNER is null then 'Unmanaged'
                else 'Managed'
            end as SALES_OWNERSHIP,
            DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) as MONTH_TRANSACTED,
            case
                when CARDUP_PAYMENT_CARD_TYPE = '' then 'Bank Transfer'
                else CARDUP_PAYMENT_CARD_TYPE
            end as CARD_TYPE,
            CARDUP_PAYMENT_BIN,
            CARDUP_PAYMENT_PAYMENT_TYPE as PAYMENT_TYPE,
            INDUSTRY,
            case
                when CARDUP_PAYMENT_NEXT_DAY_FEE > 0 then 1
                else 0
            end as NEXT_DAY,
            DENSE_RANK() OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
                order by
                    DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) DESC
            ) as LATEST_MONTH_RANK,
            COUNT(distinct DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS))) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) as MONTH_ACTIVE_COUNT,
            SUM(CARDUP_PAYMENT_USD_AMT) as TOTAL_GTV,
            SUM(CARDUP_PAYMENT_TOTAL_REVENUE_USD_AMT) as TOTAL_REVENUE,
            SUM(CARDUP_PAYMENT_NET_REVENUE_USD_AMT) as TOTAL_NET_REVENUE,
            SUM(CARDUP_PAYMENT_CARDUP_FEE / (CARDUP_PAYMENT_LCL_AMT / CARDUP_PAYMENT_USD_AMT)) as TOTAL_CARDUP_FEE,
            SUM(CARDUP_PAYMENT_TOTAL_COST_USD_AMT) as TOTAL_COST,
            count(distinct DWH_CARDUP_PAYMENT_ID) as TOTAL_TX_COUNT,
            max(case when cardup_payment_schedule_type = 'recurring' then 1 else 0 end) as RECURRING_FLAG,
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
            inner join INDUSTRY_TABLE T2 on T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = T2.CU_COMPANY_ID
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business')
            and CARDUP_PAYMENT_CU_LOCALE_ID = 1
            and LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            --and CARDUP_PAYMENT_SALES_OWNER is not null
            and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) <= DATE('2025-01-01')
        group by
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8
    ),
    MAIN_AGGREGATED_BIN as (
        select
            COMPANY_ID,
            SALES_OWNERSHIP,
            MONTH_TRANSACTED,
            CARD_TYPE,
            INDUSTRY,
            NEXT_DAY,
            MONTH_ACTIVE_COUNT,
            COUNT(distinct PAYMENT_TYPE) COUNT_PAYTYPE_ALL_BIN,
            SUM(TOTAL_GTV) TOTAL_GTV_ALL_BIN,
            SUM(TOTAL_REVENUE) TOTAL_REVENUE_ALL_BIN,
            SUM(TOTAL_NET_REVENUE) TOTAL_NET_REVENUE_ALL_BIN,
            SUM(TOTAL_CARDUP_FEE) TOTAL_CARDUP_FEE_ALL_BIN,
            SUM(TOTAL_COST) TOTAL_COST_ALL_BIN,
            sum(TOTAL_TX_COUNT) TOTAL_TX_COUNT_ALL_BIN,
            max(RECURRING_FLAG) RECURRING_FLAG
            from MAIN_AGGREGATED_ALL
        where
            LATEST_MONTH_RANK = 1
            and MONTH_TRANSACTED >= DATE('2023-01-01')
        group by
            1,
            2,
            3,
            4,
            5,
            6,
            7
    ),
    MAIN_AGGREGATED_BIN_P6M as (
        select
            COMPANY_ID,
            SALES_OWNERSHIP,
            CARD_TYPE,
            INDUSTRY,
            NEXT_DAY,
            MONTH_ACTIVE_COUNT,
            COUNT(distinct PAYMENT_TYPE) COUNT_P6M_PAYTYPE_ALL_BIN,
            avg(TOTAL_GTV) AVG_P6M_GTV_ALL_BIN,
            avg(TOTAL_REVENUE) AVG_P6M_REVENUE_ALL_BIN,
            avg(TOTAL_NET_REVENUE) AVG_P6M_NET_REVENUE_ALL_BIN,
            avg(TOTAL_CARDUP_FEE) AVG_P6M_CARDUP_FEE_ALL_BIN,
            avg(TOTAL_COST) AVG_P6M_COST_ALL_BIN,
            avg(TOTAL_TX_COUNT) AVG_P6M_TX_COUNT_ALL_BIN,
            max(RECURRING_FLAG) P6M_RECURRING_FLAG
            from MAIN_AGGREGATED_ALL
        where
            LATEST_MONTH_RANK < 7
            and MONTH_TRANSACTED >= DATE('2020-01-01')
        group by
            1,
            2,
            3,
            4,
            5,
            6
    ),
    MAIN_FINAL as (
        select
            T1.COMPANY_ID,
            T1.SALES_OWNERSHIP,
            T1.MONTH_TRANSACTED,
            T1.CARD_TYPE,
            T1.CARDUP_PAYMENT_BIN,
            T1.INDUSTRY,
            T1.NEXT_DAY,
            T1.MONTH_ACTIVE_COUNT,
            case
                when AVG_P6M_GTV_ALL_BIN < 30000 then '01. <$30k'
                when AVG_P6M_GTV_ALL_BIN < 100000 then '01a. $30-100k'
                when AVG_P6M_GTV_ALL_BIN < 150000 then '02. $100-150k'
                when AVG_P6M_GTV_ALL_BIN < 200000 then '03. <$150-200k'
                when AVG_P6M_GTV_ALL_BIN >= 200000 then '04. >$200k'
                else null
            end as UNMANAGED_GTV_TIER,
            case
                when AVG_P6M_GTV_ALL_BIN < 100000 then '01. <$100k'
                when AVG_P6M_GTV_ALL_BIN < 150000 then '02. $100-150k'
                when AVG_P6M_GTV_ALL_BIN < 200000 then '03. <$150-200k'
                when AVG_P6M_GTV_ALL_BIN >= 200000 then '04. >$200k'
                else null
            end as MANAGED_GTV_TIER,
            TOTAL_GTV_ALL_BIN,
            TOTAL_REVENUE_ALL_BIN,
            TOTAL_NET_REVENUE_ALL_BIN,
            TOTAL_CARDUP_FEE_ALL_BIN,
            TOTAL_COST_ALL_BIN,
            TOTAL_TX_COUNT_ALL_BIN,
            TOTAL_NET_REVENUE_ALL_BIN / TOTAL_GTV_ALL_BIN as TAKE_RATE_ALL_BIN,
            TOTAL_COST_ALL_BIN / TOTAL_GTV_ALL_BIN as COST_RATE_ALL_BIN,
            TOTAL_CARDUP_FEE_ALL_BIN / TOTAL_GTV_ALL_BIN as CARDUP_FEE_ALL_BIN,
            TOTAL_GTV,
            TOTAL_REVENUE,
            TOTAL_NET_REVENUE,
            TOTAL_CARDUP_FEE,
            TOTAL_COST,
            TOTAL_TX_COUNT,
            TOTAL_NET_REVENUE / TOTAL_GTV as TAKE_RATE,
            TOTAL_COST / TOTAL_GTV as COST_RATE,
            TOTAL_CARDUP_FEE / TOTAL_GTV as CARDUP_FEE,
            T2.COUNT_PAYTYPE_ALL_BIN,
            T3.MONTH_ACTIVE_COUNT_COLLECT,
            T3.TOTAL_TX_COUNT_COLLECT,
            T1.RECURRING_FLAG,
            COUNT_P6M_PAYTYPE_ALL_BIN,
            AVG_P6M_GTV_ALL_BIN,
            AVG_P6M_REVENUE_ALL_BIN,
            AVG_P6M_NET_REVENUE_ALL_BIN,
            AVG_P6M_CARDUP_FEE_ALL_BIN,
            AVG_P6M_COST_ALL_BIN,
            AVG_P6M_TX_COUNT_ALL_BIN,
            P6M_RECURRING_FLAG,
            AVG_P6M_NET_REVENUE_ALL_BIN / AVG_P6M_GTV_ALL_BIN as TAKE_RATE_ALL_BIN_P6M,
            AVG_P6M_COST_ALL_BIN / AVG_P6M_GTV_ALL_BIN as COST_RATE_ALL_BIN_P6M,
            AVG_P6M_CARDUP_FEE_ALL_BIN / AVG_P6M_GTV_ALL_BIN as CARDUP_FEE_ALL_BIN_P6M,
            
        from
            MAIN_AGGREGATED_ALL T1
            
            left outer join MAIN_AGGREGATED_BIN T2 on T1.COMPANY_ID = T2.COMPANY_ID
            and T1.SALES_OWNERSHIP = T2.SALES_OWNERSHIP
            and T1.MONTH_TRANSACTED = T2.MONTH_TRANSACTED
            and T1.CARD_TYPE = T2.CARD_TYPE
            and T1.INDUSTRY = T2.INDUSTRY
            and T1.NEXT_DAY = T2.NEXT_DAY
            
            left outer join COLLECT_AGGREGATED_ALL T3  on T1.COMPANY_ID = T3.COMPANY_ID
            and T1.SALES_OWNERSHIP = T3.SALES_OWNERSHIP
            and T1.MONTH_TRANSACTED = T3.MONTH_TRANSACTED
            and T3.LATEST_MONTH_RANK = 1

            left outer join MAIN_AGGREGATED_BIN_P6M T4 on T1.COMPANY_ID = T4.COMPANY_ID
            and T1.SALES_OWNERSHIP = T4.SALES_OWNERSHIP
            and T1.CARD_TYPE = T4.CARD_TYPE
            and T1.INDUSTRY = T4.INDUSTRY
            and T1.NEXT_DAY = T4.NEXT_DAY

        where
            T1.LATEST_MONTH_RANK = 1
            and T1.MONTH_TRANSACTED >= DATE('2023-01-01')
            and T1.CARD_TYPE in ('Visa', 'Mastercard')
    )
select
    *
from
    MAIN_FINAL
    where industry is not null;