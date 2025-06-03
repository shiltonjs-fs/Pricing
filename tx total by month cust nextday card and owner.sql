--by month, by customer, by next day or not, by card, and by sales owner
select
    DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) as MONTH_TRANSACTED,
    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID as COMPANY_ID,
    CARDUP_PAYMENT_SALES_OWNER as SALES_OWNER,
    CARDUP_PAYMENT_CARD_TYPE as CARD_TYPE,
    case
        when CARDUP_PAYMENT_NEXT_DAY_FEE > 0 then 1
        else 0
    end as NEXT_DAY,
    SUM(CARDUP_PAYMENT_USD_AMT) as TOTAL_GTV,
    SUM(CARDUP_PAYMENT_TOTAL_REVENUE_USD_AMT) as TOTAL_REVENUE
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
WHERE
    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
    --AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
    AND CARDUP_PAYMENT_USER_TYPE IN ('business')
    and CARDUP_PAYMENT_CU_LOCALE_ID = 1
    and LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2023-01-01')
group by
    1,
    2,
    3,
    4,
    5;