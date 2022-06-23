import pandas as pd
import pandasql as ps


def create_df(file_name:str):
        df= pd.read_csv('/Users/oz.barlev/Downloads/lusha-de-interview/data/'+file_name+'.csv')
        return df

def replace_names(df,original,sub):
        df.columns=df.columns.str.replace(original,sub)


#create the tables
purchases=create_df('purchases')
users_utm=create_df('users_utm')
users=create_df('users')

# removing empty spaces in headers
replace_names(purchases,' userId', 'userId')
replace_names(users_utm,' userId', 'userId')
replace_names(users_utm,' utmSource', 'utmSource')


purchases['Billing_amount'] = pd.to_numeric(purchases[' Billing_amount ($)'])
purchases['purchaseDate'] = pd.to_datetime(purchases['purchaseDate']).dt.date
users_utm['utmDate'] = pd.to_datetime(users_utm['utmDate']).dt.date
users['registrationDate'] = pd.to_datetime(users['registrationDate']).dt.date


q1 = """
        with sum_billing as (
        SELECT uu.userId,uu.utmSource, Billing_amount/2/entries.count_entries as quart
        FROM users_utm uu left join users u on uu.userId=u.userId 
        left join purchases p on uu.userId=p.userId 
        join (SELECT uu.userId,count(uu.utmSource)-1 as count_entries
        FROM users_utm uu
        group by 1) entries on entries.userId=uu.userId ),

        main as(
        SELECT uu.utmDate,uu.userId,uu.utmSource,u.registrationDate,p.purchaseDate,p.Billing_amount
        FROM users_utm uu left join users u on uu.userId=u.userId and utmDate=registrationDate
        left join purchases p on uu.userId=p.userId and utmDate=purchaseDate),

        aggregated_billings as(
        SELECT distinct uu.userId,uu.utmSource,p.Billing_amount, COALESCE(p.Billing_amount/2,sb.quart) as price
        FROM users_utm uu left join users u on uu.userId=u.userId and utmDate=registrationDate
        left join purchases p on uu.userId=p.userId and utmDate=purchaseDate
        join sum_billing sb on uu.userId=sb.userId and uu.utmSource=sb.utmSource),

        sum_billings as (
        select utmSource,sum(ab.price) as total_billing
        from aggregated_billings ab
        group by 1
        )
        
        select max(m.utmDate) as Calendardate,m.utmSource, count(m.registrationDate) as number_of_registrations,count(m.purchaseDate) as number_of_purchases, sb.total_billing
        from main as m join sum_billings as sb on sb.utmSource=m.utmSource
        group by 2

        """
print(ps.sqldf(q1, locals()))


# # **** Draft table ****
# # this table is for internal use, help to see the whole picture
# q_big_table = """
#         SELECT uu.utmDate,uu.userId,uu.utmSource,u.registrationDate,p.purchaseDate,p.Billing_amount,entries.count_entries_minus1
#         FROM users_utm uu left join users u on uu.userId=u.userId and utmDate=registrationDate
#         left join purchases p on uu.userId=p.userId and utmDate=purchaseDate
#         join (SELECT uu.userId,count(uu.utmSource)-1 as count_entries_minus1
#         FROM users_utm uu
#         group by 1) entries on entries.userId=uu.userId"""
# print(ps.sqldf(q_big_table, locals()))
