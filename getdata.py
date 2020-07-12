from jqdatasdk import *
auth('15377557502','557502')
import pandas as pd
import sys
import numpy as np
sys.path.insert(1,'c:\\Users\\Handel\\Documents\\quant\\linearmodels\\')
import  linearmodels 

class GeoMomnt():
    def __init__(self,city,indu,end=None,unit='1M',count=120):
        self.city = city
        self.indu = indu
        self.end = end
        self.unit = unit
        self.count = count
        self.std_date = self.get_standard_date()
        self.begin_date = self.std_date.iloc[0,0].strftime('%Y-%m-%d')
        self.loc_indu = self.industry_local_codes()

    # 以下industry 分类采用申万一级行业
    def nonlocal_industry_codes(self):
        ids_stocks = get_industry_stocks(self.indu, date=self.end)
        out =  [i for i in ids_stocks if i not in self.loc_indu]
        return self.clean_stocks(out)

    def nonindustry_local_codes(self):
        loc_stocks = finance.run_query(query(finance.STK_COMPANY_INFO.code).filter(finance.STK_COMPANY_INFO.city==self.city).limit(1000))
        loc_stocks = loc_stocks['code'].tolist()
        out = [i for i in loc_stocks if i not in self.loc_indu]
        return self.clean_stocks(out)

    def industry_local_codes(self):
        ids_stocks = get_industry_stocks(self.indu,date=self.end)
        info = finance.run_query(query(finance.STK_COMPANY_INFO.code).filter(finance.STK_COMPANY_INFO.code.in_(ids_stocks),finance.STK_COMPANY_INFO.city==self.city).limit(100)) 
        ids_loc_stocks = info['code'].tolist()
        return self.clean_stocks(ids_loc_stocks)

    def loc_indu_stay(self):
        loc_indu_end = self.loc_indu

        loc_indu_begin = get_industry_stocks(self.indu,date=self.begin_date)
        return [i for i in loc_indu_end if i in loc_indu_begin]

    def clean_stocks(self,list):
        out = []
        for i in list:
            if i.split('.')[0].isnumeric():
                out.append(i)
        return out

    def get_portfolio_return(self,stocks_list):
        # input a list of stocks' code, return a Series of equal weighted return
        #stocks_price = get_bars(stocks_list,self.count,unit=self.unit,fields=['date','close'],end_dt=self.end)
        std_dt = self.std_date #A Dataframe of standard date
        stocks_return = std_dt.shift(1)
        for idv in stocks_list:
            try:
                idv_price = get_bars(idv,self.count,unit=self.unit,fields=['date','close'],end_dt=self.end)
            except:
                print(idv)
                continue
            #merge with standard date and calculate the return rate
            merged = pd.merge(std_dt,idv_price,how='left',on='date')
            new_idv_price = merged['close'] #Series
            idv_return = new_idv_price.diff(1)/new_idv_price.shift(1)

            idv_return = idv_return.rename(idv) # rename the stock's return with its code
            df_return = pd.concat([std_dt['date'].shift(1),idv_return],axis=1) #concat return and timeline
            stocks_return = pd.merge(stocks_return,df_return,how='left',on='date') #dataframe

        pf_return = stocks_return.mean(axis=1,skipna=True) #series
        return [stocks_return,pf_return]

    def get_standard_date(self):
        #some stocks' price are lacked in specific months
        #get a standard date from '000001.XSHG' to set a time base line
        standard_date_df = get_bars('000001.XSHG',self.count,unit=self.unit,fields=['date'],end_dt=self.end)
        return standard_date_df


    def fdmt_df(self,stocks,begin):
        q = query(valuation.code,valuation.market_cap,
        (balance.total_owner_equities/valuation.market_cap/100000000.0).label("BTM"),indicator.roe,
        balance.total_assets.label("Inv")).filter(valuation.code.in_(stocks))
        df = get_fundamentals(q,begin)
        return df

    def FM(self,indu_nonloc=None,loc_nonindu=None):
        x = self.std_date.shift(1)
        if indu_nonloc is None:
            indu_nonloc_stocks = self.nonlocal_industry_codes()
        else:
            indu_nonloc_stocks = indu_nonloc
            
        if loc_nonindu is None:
            loc_nonindu_stocks = self.nonindustry_local_codes()
        else:
            loc_nonindu_stocks = loc_nonindu

        #剔除期间新上市的股票
        loc_indu = self.loc_indu_stay()
        entity_len = len(loc_indu)
        # y is time by entity, x1, x2 are Series
        [y,pf_retun] = self.get_portfolio_return(loc_indu) 
        [indu_nonloc_return,x1] = self.get_portfolio_return(indu_nonloc_stocks)
        [loc_nonindu_return,x2] = self.get_portfolio_return(loc_nonindu_stocks)
        date_x1 = pd.concat([self.std_date.shift(1),x1.rename('industry')],axis=1)
        date_x1_x2 = pd.concat([date_x1,x2.rename('local')],axis=1)
        #merge y-x1-x2-lagged_y 
        temp1 = pd.merge(y,date_x1_x2,how='left',on='date')
        lagged_y = y
        lagged_y.iloc[:,1:] = lagged_y.iloc[:,1:].shift(1)
        temp2 = pd.merge(temp1,lagged_y,how='left',on='date')
        temp2 = temp2.dropna()
        #剔除之后的数据
        y = temp2.iloc[:,1:1+entity_len].as_matrix()
        x1 = temp2.iloc[:,1+entity_len]
        x2 = temp2.iloc[:,2+entity_len]
        factor3_df = temp2.iloc[:,3+entity_len:]
        factor1 = [x1,]*entity_len
        factor1_df = pd.concat(factor1,axis=1,keys=loc_indu)
        factor2 = [x2,]*entity_len
        factor2_df = pd.concat(factor2,axis=1,keys=loc_indu)
        factors_m = np.array([factor1_df.as_matrix(),factor2_df.as_matrix(),factor3_df.as_matrix()])
        # x = pd.merge(x,x1,how='left',on='date')
        # x = pd.merge(x,x2,how='left',on='date')
        # panel = pd.merge(y,x,how='left',on='date')
        # panel = panel.dropna()
        
        # res = linearmodels.FamaMacBeth(y,x)
        return [y,factors_m]

    def panel_yx(self):
        pass