--by card, next day, GTV tier
with
    X as (
        select
            DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) as MONTH_TRANSACTED,
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID as COMPANY_ID,
            case when CARDUP_PAYMENT_CARD_TYPE = '' then 'Bank Transfer' else CARDUP_PAYMENT_CARD_TYPE end as CARD_TYPE,
            case
                when CARDUP_PAYMENT_NEXT_DAY_FEE > 0 then 1
                else 0
            end as NEXT_DAY,
            SUM(CARDUP_PAYMENT_USD_AMT) as TOTAL_GTV,
            SUM(CARDUP_PAYMENT_TOTAL_REVENUE_USD_AMT) as TOTAL_REVENUE,
            SUM(CARDUP_PAYMENT_NET_REVENUE_USD_AMT) as TOTAL_NET_REVENUE,
            sum(CARDUP_PAYMENT_CARDUP_FEE/(CARDUP_PAYMENT_LCL_AMT/CARDUP_PAYMENT_USD_AMT)) as TOTAL_CARDUP_FEE,
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
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
            4
    )
select
    case
        when TOTAL_GTV < 100000 then '01. $0-100,000'
        when TOTAL_GTV < 150000 then '02. $100,000-150,000'
        when TOTAL_GTV < 200000 then '03. $150,000-200,000'
        when TOTAL_GTV >= 200000 then '04. >$200,000'
        else null
    end as GTV_BUCKET,
    CARD_TYPE,
    NEXT_DAY,
    SUM(TOTAL_GTV) TOTAL_GTV,
    SUM(TOTAL_CARDUP_FEE) TOTAL_CARDUP_FEE,
    SUM(TOTAL_REVENUE) TOTAL_REVENUE,
    SUM(TOTAL_NET_REVENUE) TOTAL_NET_REVENUE,
from
    X
group by
    1,
    2,
    3;