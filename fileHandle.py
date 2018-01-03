# -*- coding: utf-8 -*-
"""
Created on Thu May 18 14:31:11 2017

@author: Administrator
"""

import pandas as pd
import os
from sqlalchemy import create_engine 
import numpy as np
import pymysql
import glob
import sys
import time
sys.path.append("..")
reload(sys)
sys.setdefaultencoding('utf8')

from DataHanle.stockhandle import StockHandle
from Strategy.colligation import ZH


class fileHandle():
    def __init__(self):
        
        self.rdir=os.path.dirname(os.getcwdu())+'\\Data\\History\\Stock\\股票历史数据\\1min\\'
        
        #self.tdir=os.path.dirname(os.getcwdu())+'\\Data\\History\\Stock\\股票历史数据\\补全数据1min\\'
        
        self.tdir=u'E:\\work\\股票数据\\股票5min\\'#股票1min
        
        self.rootdir=os.path.dirname(os.getcwdu())+'\\Data\\History\\Stock\\股票历史数据\\'

        self.engine = create_engine('mysql://root:lzg000@127.0.0.1/stocksystem?charset=utf8')
        
        self.tickdir=os.path.dirname(os.getcwdu())+'\\Data\\History\\Stock\\股票tick数据'
        
        self.tdxstockd=u'E:\\work\\股票数据\\股票日线\\'
        
        self.tdxindexd=u'E:\\work\\股票数据\\板块日线\\'
        
        self.tdxstock5min=u'E:\\work\\股票数据\\股票5min\\'
        
        self.tdxindex5min=u'E:\\work\\股票数据\\板块5min\\'
        
        self.tdxdir=u'E:\\work\\股票数据\\'
        
        self.errortxt=open(u'E:\\work\\股票数据\\test\\errorcode.txt','a')
        
        self.conn=pymysql.connect(host='localhost',
                user='root',
                passwd='lzg000',
                db='stocksystem',
                port=3306,
                charset='utf8'
                )    
        self.cursor=self.conn.cursor()        
        
        self.ohlc_dict = {           
            'hq_date':'last',
            'hq_time':'last',
            'hq_code':'first',
            'hq_open':'first',                                                                                                    
            'hq_high':'max',                                                                                                       
            'hq_low':'min',                                                                                                        
            'hq_close': 'last',                                                                                                    
            'hq_vol': 'sum' ,   
            'hq_amo': 'sum'        
            }
              
        self.ohlc_dict2 = {           
            'hq_date':'last',
            'hq_code':'first',
            'hq_open':'first',                                                                                                    
            'hq_high':'max',                                                                                                       
            'hq_low':'min',                                                                                                        
            'hq_close': 'last',                                                                                                    
            'hq_vol': 'sum' ,   
            'hq_amo': 'sum'        
            }            
        
        
    #补齐1分钟数据   
    def fill1min(self):
        
        flist=os.listdir(self.rdir)
        num=0
        for f in flist:            
            fname=os.path.join(self.rdir,f)  
            tname=os.path.join(self.tdir,f)
            s1=pd.read_csv(fname,index_col=0)
            
            s1['hq_close'].fillna(method='ffill',inplace=True)
            s1['hq_open'].fillna(method='ffill',inplace=True)
            s1['hq_high'].fillna(method='ffill',inplace=True)
            s1['hq_low'].fillna(method='ffill',inplace=True)
            
            s1['hq_close'].fillna(method='bfill',inplace=True)
            s1['hq_open'].fillna(method='bfill',inplace=True)
            s1['hq_high'].fillna(method='bfill',inplace=True)
            s1['hq_low'].fillna(method='bfill',inplace=True)
            
            s1['hq_amo'].fillna(0,inplace=True)
            s1['hq_vol'].fillna(0,inplace=True)
            s1.to_csv(tname)
            num+=1
            print '补齐'
            print num    
            
            
    def cl30(self,x):
        y=pd.DataFrame()
        theindex=x.index
        
        for i in xrange(4):

            x.loc[theindex[2*i+1],'hq_open']=x.iloc[2*i]['hq_open']
            x.loc[theindex[2*i+1],'hq_high']=x.iloc[2*i:2*i+2]['hq_high'].max()
            x.loc[theindex[2*i+1],'hq_low']=x.iloc[2*i:2*i+2]['hq_low'].min()
            x.loc[theindex[2*i+1],'hq_amo']=x.iloc[2*i:2*i+2]['hq_amo'].sum()
            x.loc[theindex[2*i+1],'hq_vol']=x.iloc[2*i:2*i+2]['hq_vol'].sum()
            y=y.append(x.iloc[2*i+1:2*i+2])

        return y           
           
           
    def groupmin(self,s1,week):
        
        
#        s5=s1.resample('5T', label='right',closed='right').apply(self.ohlc_dict)          
#        s5=s5.dropna(axis=0).drop(s5[s5.hq_time=='13:00'].index)    
        
        s15=s1.resample('15T', label='right',closed='right').apply(self.ohlc_dict) 
        s15=s15.dropna(axis=0).drop(s15[s15.hq_time=='13:00'].index)
        
        s30=s1.resample('30T', label='right',closed='right').apply(self.ohlc_dict) 
        s30=s30.dropna(axis=0).drop(s30[s30.hq_time=='13:00'].index)
                
        s60=s30.groupby('hq_date').apply(self.cl30)
        #del s60['hq_date']
        s60.reset_index(level=0,inplace=True,drop=True)

#        sday=s1.resample('D', label='left',closed='right').apply(self.ohlc_dict2) 
#        sday.dropna(axis=0,inplace=True)

        return s15,s30,s60
    
    def group_week(self,s1):
        
        sweek=s1.resample('W-FRI').apply(self.ohlc_dict2)
        
        sweek.dropna(axis=0,inplace=True)       
        
        return sweek        
        
    #聚合5分钟数据，生成CSV,入库        
    def groupFile(self,date='',week=False):
 

        def cl(x):
            x.ix[1:2,['hq_vol','hq_amo']]=x.ix[0:1,['hq_vol','hq_amo']].values+x.ix[1:2,['hq_vol','hq_amo']].values
            x=x[1:]
            return x
        
        names=['hq_date','hq_time','hq_open','hq_high','hq_low','hq_close','hq_vol','hq_amo']
           
        flist=os.listdir(self.tdir)
        
        n=0
        for f in flist:
            
#            if 'S' in f:
#                
#                code = filter(lambda x:x not in '#SZH',f)[:-4]
#            else:  
                
            code=f[:-4]
                
            fname=os.path.join(self.tdir,f)    
                             
#            try:
#                s1=pd.read_table(fname,header=1,usecols=[0,1,2,3,4,5,6],names=names,dtype={'hq_time':str,'hq_close':np.float,'hq_open':np.float,'hq_high':np.float,'hq_low':np.float,'hq_vol':np.float,'hq_amo':np.float},encoding='utf-8').dropna()
#            except:
#                s1=pd.read_table(fname,header=1,usecols=[0,1,2,3,4,5,6],names=names,dtype={'hq_time':str,'hq_close':np.float,'hq_open':np.float,'hq_high':np.float,'hq_low':np.float,'hq_vol':np.float,'hq_amo':np.float},encoding='gbk').dropna()  
#                
            #读CSV的句子，先注释掉
            #s1=pd.read_csv(fname,index_col=0)
            #s1.index=pd.to_datetime(s1.index)
            
            #读通达信的分钟文件
            s1=pd.read_table(fname,header=1,usecols=[0,1,2,3,4,5,6,7],names=names,dtype={'hq_time':str,'hq_close':np.float,'hq_open':np.float,'hq_high':np.float,'hq_low':np.float,'hq_vol':np.float,'hq_amo':np.float},encoding='gbk').dropna()
            s1['hq_time']=s1['hq_time'].apply(lambda x :x[0:2]+':'+x[2:4])
            s1.index=pd.to_datetime(s1['hq_date']+' '+s1['hq_time'])
            s1['hq_code']=code
            #s1.drop(s1[s1.hq_date=='2017-09-21'].index,inplace=True)


            if f==flist[0]:
                
                dateExample=str(s1['hq_date'].iat[0])
                
                if '/' in dateExample:
                    
                    date=date.replace('-','/')
                    
                    
            if len(date)>2:
                s1=s1[s1.hq_date>=date]
            
            if len(s1)<48:
                
                print code+' 数据长度不足'
                
                continue
            

            if week==True:           
                try:
                    sweek=self.group_week(s1)
                except:
                    self.errortxt.write(code+'数据有误\n')    
                
                sweek.to_sql('hstockquotationweek',self.engine,if_exists='append')
                    
            else:               
                try:
                    s15,s30,s60=self.groupmin(s1)              
                except:
                   self.errortxt.write(code+'数据有误\n')
                
                s15.to_sql('hstcokquotationfifteen',self.engine,if_exists='append') 
                s30.to_sql('hstockquotationthirty',self.engine,if_exists='append') 
                s60.to_sql('hstockquotationsixty',self.engine,if_exists='append')

            f=f.replace('.txt','.csv')
#            if n==0:                           
#                s5.to_csv(self.rootdir+'5min\\'+f,mode='w')
#                s15.to_csv(self.rootdir+'15min\\'+f,mode='w')
#                s60.to_csv(self.rootdir+'60min\\'+f,mode='w')
#                sday.to_csv(self.rootdir+'day\\'+f,mode='w')
#                sweek.to_csv(self.rootdir+'week\\'+f,mode='w')
#                                              
#            else: 
#                s5.to_csv(self.rootdir+'5min\\'+f,mode='a',header=False)
#                s15.to_csv(self.rootdir+'15min\\'+f,mode='a',header=False)
#                s60.to_csv(self.rootdir+'60min\\'+f,mode='a',header=False)
#                sday.to_csv(self.rootdir+'day\\'+f,mode='a',header=False)
#                sweek.to_csv(self.rootdir+'week\\'+f,mode='a',header=False)     


#            if week==True:
#                
#                sweek.to_sql('hstockquotationweek',self.engine,if_exists='append') 
#            
#            else:         
#                #s1.to_sql('hstcokquotationone',self.engine,if_exists='append')
###                #s5.to_sql('hstockquotationfive',self.engine,if_exists='append') 
#                s15.to_sql('hstcokquotationfifteen',self.engine,if_exists='append') 
#                s30.to_sql('hstockquotationthirty',self.engine,if_exists='append') 
#                s60.to_sql('hstockquotationsixty',self.engine,if_exists='append')  
                #sday.to_sql('hstockquotationday',self.engine,if_exists='append') 
#                     
            n+=1
        
            print str(n)+' '+code
            
        self.errortxt.close()

        
    #对1分钟数据进行入库，入库目录为补全数据1min
    def loading1min(self):
        i=0
        s2=pd.DataFrame()
        filelist=os.listdir(self.tdir)
        lastfile=filelist[-1]
        for f in  filelist:
            filename=os.path.join(self.tdir,f)
            s1=pd.read_csv(filename,index_col=0,dtype={'hq_time':str,'hq_date':str,'hq_close':np.float,'hq_open':np.float,'hq_high':np.float,'hq_low':np.float,'hq_vol':np.float,'hq_amo':np.float})
            s2=s2.append(s1)
            i+=1
            print '1min入库'
            print i
            if i%10==0:
                s2.to_sql('hstockquotationone',con=self.engine,if_exists='append')
                s2=pd.DataFrame()
            elif f==lastfile:
                s2.to_sql('hstockquotationone',con=self.engine,if_exists='append')    
                
                
    #找到小于1KB的文件，输入参数：年份（字符串）    
    def find0kbFile(self,year):
        ydir=os.path.join(self.tickdir,year)
        mdirlist=[]
        mlist=os.listdir(ydir)
        for m in mlist:
            mdir=os.path.join(ydir,m)
            mdirlist.append(mdir)
            
        #获得日期目录列表
        ddirlist=[]
        for mdir in mdirlist:
            dlist=os.listdir(mdir)
            for d in dlist:
                ddir=os.path.join(mdir,d)
                ddirlist.append(ddir)
        
        #找到日期目录
        for ddir in ddirlist:
            for f in os.listdir(ddir):
                fname=os.path.join(ddir,f)
                #找到小于0.1K的文件，输出
                fsize=float(os.path.getsize(fname))
                fsize=fsize/1024
                if fsize<0.01:
                    print fname        
                  
                  
    #找到缺失的文件，，输入参数：年份（字符串）                    
    def findMissingFile(self,year):
        ydir=os.path.join(self.tickdir,year)
        missfile=os.path.join(self.tickdir,u'tick缺失','missingdata.txt')
        trdset=set()
        tickset=set()
        mdirlist=[]
        mlist=os.listdir(ydir)
        for m in mlist:
            mdir=os.path.join(ydir,m)
            mdirlist.append(mdir)
            
        #获得日期目录列表
        ddirlist=[]
        for mdir in mdirlist:
            dlist=os.listdir(mdir)
            for d in dlist:
                ddir=os.path.join(mdir,d)
                ddirlist.append(ddir)
       
        #找到日期目录
        num=0
        for ddir in ddirlist:
            day=(ddir.split('\\'))[-1]
            for f in os.listdir(ddir):
                code=f[2:-4]
                tickset.add(code)
                codeList=pd.read_sql("SELECT code FROM stockday where hq_date =str_to_date('"+day+"', '%Y-%m-%d %H')",con=self.engine)
                length=len(codeList)
                for i in xrange(length):
                        code=codeList.iat[i,0]
                        trdset.add(code)
                    
                difference=trdset-tickset
                if difference:
                    fin=open(missfile,'a')
                    fin.write(day+'\n')
                    for line in difference:
                        fin.write(line+'\n')
                    fin.write('--------------------------------------------\n')
                    fin.close()
                    print '有差'          
            num+=1
            print num
                    
                    
    def tdxFileLoading(self,linetype,indexflag,cursor,date='',mintype=5,delete=False):
        
        if linetype=='D' or linetype=='d':
            
            if indexflag==0:
                jysjpth=self.tdxstockd
                names=['hq_date','hq_open','hq_high','hq_low','hq_close','hq_vol','hq_amo']
                table='hstockquotationday'                
                truncatesql='truncate table '+table
                #table='stockday'
            else:
                jysjpth=self.tdxindexd
                table='hindexquotationday'
                names=['hq_date','hq_open','hq_high','hq_low','hq_close','hq_vol','hq_amo']
                truncatesql='truncate table '+table
               
            if delete==True:           
                cursor.execute(truncatesql)
                self.conn.commit()
                print truncatesql
                    
            engine =self.engine
            
            i=0
            s2=pd.DataFrame()
            filelist=os.listdir(jysjpth)
            lastfile=filelist[-1]
            #s=filelist.index('SZ300617.txt')
            #filelist=filelist[s+1:]
            for f in  filelist:
                
                filename=jysjpth+f

                code=f[:-4]
                
                if code == 'HZ5044':
                    
                    code = 105044
                
                elif code == 'HZ5014':
                    
                    code = 105014
            
                elif code == 'HSI':
                    
                    code = 100001

                try:
                    s1=pd.read_table(filename,header=1,usecols=[0,1,2,3,4,5,6],names=names,dtype={'hq_time':str,'hq_close':np.float,'hq_open':np.float,'hq_high':np.float,'hq_low':np.float,'hq_vol':np.float,'hq_amo':np.float},encoding='utf-8').dropna()
                except:
                    s1=pd.read_table(filename,header=1,usecols=[0,1,2,3,4,5,6],names=names,dtype={'hq_time':str,'hq_close':np.float,'hq_open':np.float,'hq_high':np.float,'hq_low':np.float,'hq_vol':np.float,'hq_amo':np.float},encoding='gbk').dropna()
                
                if len(date)>2 and delete == False:
                    s1=s1[s1['hq_date']==date]
                #s1["hq_date"]=s1["hq_date"].apply(lambda x: x.strip().replace('/',''))
                s1['hq_code']=code
                i+=1
                s2=s2.append(s1)
            
                if i%30==0:  
                    index=s2['hq_date'].tolist()
                    index=pd.to_datetime(index)
                    s2.set_index(index,inplace=True) 
                    s2.drop_duplicates(inplace=True)
                    s2.to_sql(table,con=engine,if_exists='append')
                    s2=pd.DataFrame()
                    print i
                    
                elif f==lastfile:   
                    index=s2['hq_date'].tolist()
                    index=pd.to_datetime(index)
                    s2.set_index(index,inplace=True) 
                    s2.drop_duplicates(inplace=True)
                    s2.to_sql(table,con=engine,if_exists='append')
                    s2=pd.DataFrame()
                    print '入库完成'      
        
        elif linetype == 'W' or linetype == 'w':
            if indexflag==0:
                    table='hstockquotationweek'
                    jysjpth=self.tdxdir+'股票周线\\'
                    names=['hq_date','hq_open','hq_high','hq_low','hq_close','hq_vol','hq_amo']
                    
            else:
                    table='hindexquotationweek'
                    jysjpth=self.tdxdir+'板块周线\\'
                    names=['hq_date','hq_open','hq_high','hq_low','hq_close','hq_vol']
                     
            truncatesql='truncate table '+table        
                    
            if delete==True:           
                cursor.execute(truncatesql) 
                self.conn.commit()
                print truncatesql 


            s2=pd.DataFrame()
            filelist=os.listdir(jysjpth)
            lastfile=filelist[-1]
            i=0
            for f in  filelist:
                filename=jysjpth+f
                code=f[:-4] 
                
                
                if code == 'HZ5044':
                    
                    code = 105044
                
                elif code == 'HZ5014':
                    
                    code = 105014
            
                elif code == 'HSI':
                    
                    code = 100001

                s1=pd.read_table(filename,header=1,usecols=[0,1,2,3,4,5],names=names,dtype={'hq_date':np.str,'hq_close':np.float,'hq_vol':np.float,'hq_amo':np.float}).dropna()
    
                s1['hq_code']=code
    
                i+=1
                s2=s2.append(s1)
                if i%30==0:   
                    #index=pd.to_datetime(s2['hq_date'])       
                    #s2.set_index(index,inplace=True)     
                    #s2.dropna(axis=0,inplace=True)      
                    s2.index=s2.hq_date
                    s2.index.set_names('index',inplace=True)
                    s2.to_sql(table,con=self.engine,if_exists='append')
                    s2=pd.DataFrame()
                    print i
                elif f==lastfile: 
                    #index=s2['hq_date']
                    #s2.set_index(index,inplace=True) 
                    #s2.dropna(axis=0,inplace=True)
                    s2.index=s2.hq_date
                    s2.index.set_names('index',inplace=True)
                    s2.to_sql(table,con=self.engine,if_exists='append')
                    s2=pd.DataFrame()
                    print '入库完成' 

    
        elif linetype == 'M' or linetype == 'm':
            if indexflag==0:
                if mintype==1:
                    table='hstockquotationone'
                    jysjpth=u'E:\\股票数据\\股票1min\\'
                    indexName='1分钟线'     
                    
                elif mintype==5:  
                    table='hstockquotationfive'
                    jysjpth=self.tdxstock5min
                    indexName='5分钟线' 
                 
            else:
                if mintype==1:
                    table='hindexquotationone'
                    jysjpth=u'E:\\股票数据\\板块1min\\'
                    indexName='1分钟线'    
                    
                elif mintype==5:
                    table='hindexquotationfive'
                    jysjpth=self.tdxindex5min
                    indexName='5分钟线' 
                    
            truncatesql='truncate table '+table   
            
            if delete==True:           
                cursor.execute(truncatesql) 
                self.conn.commit()
                print truncatesql        
            engine = create_engine(r"mysql+mysqldb://root:lzg000@127.0.0.1/stocksystem?charset=utf8")
            i=0
            s2=pd.DataFrame()
            filelist=os.listdir(jysjpth)
            lastfile=filelist[-1]
            names=['hq_date','hq_time','hq_open','hq_high','hq_low','hq_close','hq_vol','hq_amo']
            
            for f in  filelist:
                
                filename=jysjpth+f

                code=f[:-4] 

                if code == 'HZ5044':
                    
                    code = 105044
                
                elif code == 'HZ5014':
                    
                    code = 105014
            
                elif code == 'HSI':
                    
                    code = 100001
#                fin=open(filename,'r')
#                codename=fin.readline().strip()
#                codename=codename.split() 
            #    if len(codename)>4:
            #        endpos=codename.index(indexName)
            #        if endpos==3:
            #            name=codename[1]+codename[2]
            #        elif endpos==4:
            #            name=codename[1]+codename[2]+codename[3]            
            #    else:  
            #        name=codename[1]
               
                #fin.close()
                
                s1=pd.read_table(filename,header=1,usecols=[0,1,2,3,4,5,6,7],names=names,dtype={'hq_date':np.str,'hq_time':np.str,'hq_close':np.float,'hq_vol':np.float,'hq_amo':np.float}).dropna()
                
                if len(date)>2 and delete == False:
                    s1=s1[s1.hq_date==date]
                #s1=s1.iloc[:-1] 
                #s1["hq_date"]=s1["hq_date"].apply(lambda x: x.strip().replace('/',''))
                s1['hq_code']=code
                #s1['hq_name']=name
                s1['hq_time']=s1['hq_time'].apply(lambda x :x[0:2]+':'+x[2:4])
            
                i+=1
                s2=s2.append(s1)
                
                if i%30==0:   
                    index=pd.to_datetime(s2['hq_date']+' '+s2['hq_time'])       
                    s2.set_index(index,inplace=True)     
                    #s2.dropna(axis=0,inplace=True)          
                    s2.to_sql(table,con=engine,if_exists='append')
                    s2=pd.DataFrame()
                    print i
                    
                elif f==lastfile: 
                    index=s2['hq_date']+' '+s2['hq_time']  
                    s2.set_index(index,inplace=True) 
                    #s2.dropna(axis=0,inplace=True)
                    s2.to_sql(table,con=engine,if_exists='append')
                    s2=pd.DataFrame()
                    print '入库完成' 
       
        
    #把tick数据转换成各种级别数据入库
    def ticktomin(self):
        s=StockHandle()
        dirlist=s.getAllStockPath(s.sdata_hqpath)
        s.StockTickHandle(dirlist,self.engine)
        self.fill1min()
        self.loading1min()
        self.group1min()


    #将文本格式转换成UTF-8
    def convertToUtf8(self,tmpdir):
    
        fdir=glob.glob(tmpdir+'*.txt')
        n=0
        for f in fdir:
            try:
                s1=pd.read_csv(f,encoding='utf-8')
            except:
                s1=pd.read_csv(f,encoding='gbk')
            s1.to_csv(f,encoding='utf-8',index=None)
            n+=1
            print n
    
    
    #处理通达信数据入库,并生成报表
    def tdxDataloading(self,date,delete=False):
    
#        self.convertToUtf8(self.tdxstockd)
#        self.convertToUtf8(self.tdxstock5min)
#        self.convertToUtf8(self.tdxindexd)
#        self.convertToUtf8(self.tdxindex5min)
        self.tdxFileLoading('d',0,cursor=self.conn.cursor(),delete=delete,date=date)
        print '股票日线入库完成'  
        self.tdxFileLoading('m',0,cursor=self.conn.cursor(),delete=delete,date=date)        
        print '股票5min线入库完成'
        self.tdxFileLoading('d',1,cursor=self.conn.cursor(),delete=delete,date=date)
        print '行业日线入库完成'
        self.tdxFileLoading('m',1,cursor=self.conn.cursor(),delete=delete,date=date)
        print '行业5min入库完成'
       
       
if __name__=='__main__':
    
    c=fileHandle()
    
    #c.group1min()

    #cursor=c.conn.cursor()
    
    #c.tdxFileLoading('w',1,cursor=c.cursor,delete=True)
    sdate='2017-12-29'
    edate='2018-01-02' 
    
    c.groupFile(date='2017-12-29')
    
#    c.tdxDataloading(edate,delete=False)
#    #c.tdxFileLoading('d',1,cursor=c.conn.cursor(),delete=True,date=edate)
#    z=ZH(sdate,edate)
#    z.buildForms()