--profitability
with
    MAIN as (
        select
            case
                when CARDUP_PAYMENT_USD_AMT < 50000 then '01. 0-50k'
                when CARDUP_PAYMENT_USD_AMT < 100000 then '02. 50-100k'
                when CARDUP_PAYMENT_USD_AMT < 150000 then '03. 100-150k'
                when CARDUP_PAYMENT_USD_AMT < 200000 then '04. 150-200k'
                else '05. 200k+'
            end as USD_AMT_BAND,
            case
                when CARDUP_PAYMENT_NEXT_DAY_FEE > 0 then 'Next Day'
                else 'Standard'
            end as NEXT_DAY,
            CARDUP_PAYMENT_CARD_TYPE as CARD_TYPE,
            case when CARDUP_PAYMENT_PROMO_CODE is not null then 'Promo Code' else 'No Promo Code' end as PROMO_CODE,
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            DWH_CARDUP_PAYMENT_ID,
            CARDUP_PAYMENT_NET_REVENUE_USD_AMT / CARDUP_PAYMENT_USD_AMT as TAKE_RATE,
            CARDUP_PAYMENT_TOTAL_REVENUE_USD_AMT / CARDUP_PAYMENT_USD_AMT as CU_FEE,
            CARDUP_PAYMENT_TOTAL_COST_USD_AMT / CARDUP_PAYMENT_USD_AMT as PROC_COST,
            CARDUP_PAYMENT_NET_REVENUE_USD_AMT as NET_REVENUE,
            CARDUP_PAYMENT_USD_AMT as GTV
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
            join CDM.COUNTERPARTY.CARDUP_COMPANY_T T2 on T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = T2.CU_COMPANY_ID
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business')
            and LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-05-01')
            and CARDUP_PAYMENT_CU_LOCALE_ID = 1
            and CARDUP_PAYMENT_CARD_TYPE in ('Visa', 'Mastercard')
            and CU_COMPANY_L1_INDUSTRY is not null
            and cardup_payment_customer_company_id in ('721',
'1658',
'1985',
'2663',
'3254',
'3283',
'3313',
'3456',
'3457',
'3511',
'3537',
'3538',
'3592',
'3607',
'3608',
'3628',
'3643',
'3657',
'3687',
'3691',
'3692',
'3693',
'3700',
'3702',
'3707',
'3714',
'3718',
'3720',
'3750',
'3778',
'3779',
'3792',
'3812',
'3815',
'3853',
'3854',
'3865',
'3876',
'3881',
'3886',
'3889',
'3890',
'3926',
'3943',
'3945',
'4020',
'4037',
'4103',
'4112',
'4144',
'4158',
'4163',
'4216',
'4219',
'4263',
'4302',
'4349',
'4386',
'4391',
'4519',
'4563')
    )
select
    USD_AMT_BAND,
    NEXT_DAY,
    CARD_TYPE,
    PROMO_CODE,
    AVG(TAKE_RATE) as AVG_TAKE_RATE,
    APPROX_PERCENTILE(TAKE_RATE, 0.25) as Q1_TAKE_RATE,
    APPROX_PERCENTILE(TAKE_RATE, 0.5) as Q2_TAKE_RATE,
    APPROX_PERCENTILE(TAKE_RATE, 0.75) as Q3_TAKE_RATE,
    AVG(CU_FEE) as AVG_CU_FEE,
    APPROX_PERCENTILE(CU_FEE, 0.25) as Q1_CU_FEE,
    APPROX_PERCENTILE(CU_FEE, 0.5) as Q2_CU_FEE,
    APPROX_PERCENTILE(CU_FEE, 0.75) as Q3_CU_FEE,
    AVG(PROC_COST) as AVG_PROC_COST,
    APPROX_PERCENTILE(PROC_COST, 0.25) as Q1_PROC_COST,
    APPROX_PERCENTILE(PROC_COST, 0.5) as Q2_PROC_COST,
    APPROX_PERCENTILE(PROC_COST, 0.75) as Q3_PROC_COST,
    COUNT(distinct CARDUP_PAYMENT_CUSTOMER_COMPANY_ID) as TOTAL_COMPANIES,
    COUNT(distinct DWH_CARDUP_PAYMENT_ID) as TOTAL_TX_COUNT,
    AVG(GTV) as AVG_GTV,
    AVG(NET_REVENUE) as AVG_NET_REVENUE,
from
    MAIN
group by
    1,
    2,
    3,
    4;