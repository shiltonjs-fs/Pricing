--by card, next day, GTV tier
with
    MAIN as (
        select
            DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) as MONTH_TRANSACTED,
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID as COMPANY_ID,
            case
                when CARDUP_PAYMENT_CARD_TYPE = '' then 'Bank Transfer'
                else CARDUP_PAYMENT_CARD_TYPE
            end as CARD_TYPE,
            INDUSTRY,
            case
                when CARDUP_PAYMENT_NEXT_DAY_FEE > 0 then 1
                else 0
            end as NEXT_DAY,
            SUM(CARDUP_PAYMENT_USD_AMT) as TOTAL_GTV,
            --SUM(CARDUP_PAYMENT_TOTAL_REVENUE_USD_AMT) as TOTAL_REVENUE,
            SUM(CARDUP_PAYMENT_NET_REVENUE_USD_AMT) as TOTAL_NET_REVENUE,
            --sum(CARDUP_PAYMENT_CARDUP_FEE/(CARDUP_PAYMENT_LCL_AMT/CARDUP_PAYMENT_USD_AMT)) as TOTAL_CARDUP_FEE,
            ROUND(
                SUM(CARDUP_PAYMENT_CARDUP_FEE / (CARDUP_PAYMENT_LCL_AMT / CARDUP_PAYMENT_USD_AMT)) / SUM(CARDUP_PAYMENT_USD_AMT),
                4
            ) as CARDUP_FEE_RATE,
            SUM(CARDUP_PAYMENT_TOTAL_COST_USD_AMT) / SUM(CARDUP_PAYMENT_USD_AMT) as COST_RATE
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
            inner join (
                select distinct
                    CU_COMPANY_ID,
                    CU_COMPANY_L1_INDUSTRY INDUSTRY
                from
                    CDM.COUNTERPARTY.CARDUP_COMPANY_T
                where
                    COMPANY_CU_LOCALE_ID = 1
            ) T2 on T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = T2.CU_COMPANY_ID
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            --AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business')
            and CARDUP_PAYMENT_CU_LOCALE_ID = 1
            and LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2023-01-01')
            and CARDUP_PAYMENT_SALES_OWNER is not null
        group by
            1,
            2,
            3,
            4,
            5
    ),
    MAIN_WITH_TIER as (
        select
            *,
            case
                when TOTAL_GTV < 10000 then '01. $0-10,000'
                when TOTAL_GTV < 20000 then '01. $10-20,000'
                when TOTAL_GTV < 30000 then '01. $20-30,000'
                when TOTAL_GTV < 40000 then '01. $30-40,000'
                when TOTAL_GTV < 50000 then '01. $40-50,000'
                when TOTAL_GTV < 60000 then '01. $50-60,000'
                when TOTAL_GTV < 70000 then '01. $60-70,000'
                when TOTAL_GTV < 80000 then '01. $70-80,000'
                when TOTAL_GTV < 90000 then '01. $80-90,000'
                when TOTAL_GTV < 100000 then '01. $90-100,000'
                when TOTAL_GTV < 110000 then '02. $100-110,000'
                when TOTAL_GTV < 120000 then '02. $110-120,000'
                when TOTAL_GTV < 130000 then '02. $120-130,000'
                when TOTAL_GTV < 140000 then '02. $130-140,000'
                when TOTAL_GTV < 150000 then '02. $140-150,000'
                when TOTAL_GTV < 160000 then '02. $150-160,000'
                when TOTAL_GTV < 170000 then '02. $160-170,000'
                when TOTAL_GTV < 180000 then '02. $170-180,000'
                when TOTAL_GTV < 190000 then '02. $180-190,000'
                when TOTAL_GTV < 200000 then '02. $190-200,000'
                when TOTAL_GTV >= 200000 then '02. $200,000 or more'
                else null
            end as GTV_BUCKET
        from
            MAIN
        where
            TOTAL_GTV != 0
            and CARDUP_FEE_RATE != 0
            and COST_RATE != 0
    ),
    CUSTOMERS_WITH_FEE_CHANGE as (
        select
            COMPANY_ID,
            COUNT(distinct CARDUP_FEE_RATE) COUNT_UNIQUE_FEE
        from
            MAIN_WITH_TIER
        group by
            1
        having
            COUNT_UNIQUE_FEE > 1
    )
select
    *
from
    MAIN_WITH_TIER
where 
true 
    and COMPANY_ID in (
        select distinct
            COMPANY_ID
        from
            CUSTOMERS_WITH_FEE_CHANGE
   )
   and card_type = 'Visa'
    ;