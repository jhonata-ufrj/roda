#%%
import os
from pdb import set_trace
import time
import pandas
import shutil
from tqdm import tqdm
from retrying import retry
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.keys import Keys



class Tricks(object):

  VMPAY_USERNAME = 'adm diurno'
  VMPAY_PASSWORD = 'Roda@!2021'

  def pegar_infos(self, start_date=None, end_date=None,periodo=None, **kwargs):
    localdir = os.path.dirname(os.path.realpath(__file__))
    datasetdir = os.path.join(localdir, 'datasets')
    localdir = os.path.join(localdir, 'tmp')
    dataframes = list()

    if os.path.exists(localdir):
      shutil.rmtree(localdir)
    os.mkdir(localdir)
    
    if periodo != None:
      end_date= datetime.today() -timedelta(days=1)
      start_date = end_date - timedelta(days=periodo-1)


    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--incognito")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--user-agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"')

    prefs = { "download.default_directory" : localdir }
    options.add_experimental_option("prefs", prefs)

    chrome = webdriver.Chrome(options=options, executable_path='chromedriver.exe')
    chrome.get('https://rodaconveniencia.amlabs.com.br/login.jsp#produtos-mvp')
    
    # faz o login na vmpay
    chrome.find_element_by_css_selector('input[name="username"]').send_keys(self.VMPAY_USERNAME)
    
    chrome.find_element_by_css_selector('input[name="password"]').send_keys(self.VMPAY_PASSWORD)
    chrome.find_element_by_css_selector('input[type="submit"]').click()
    

    # seleciona o campo de customização
   
    
    day_begin = start_date.day  
    month_begin = start_date.month
    year_begin = start_date.year

    day_end = end_date.day  
    month_end = end_date.month
    year_end = end_date.year



    # chrome.execute_script(f'window.location.href = "https://vmpay.vertitecnologia.com.br/roda/reports/cashless_facts?cashless_fact[cashless_error_friendly][]=&cashless_fact[client_id][]=&cashless_fact[custom_end_time]={end}&cashless_fact[custom_start_time]={begin}&cashless_fact[customer_id][]=&cashless_fact[distribution_center_id][]=&cashless_fact[eft_authorizer_id][]=&cashless_fact[eft_card_brand_id][]=&cashless_fact[eft_card_type_id][]=&cashless_fact[equipment_id][]=&cashless_fact[good_category_id][]=&cashless_fact[good_id][]=&cashless_fact[good_manufacturer_id][]=&cashless_fact[groups][]=&cashless_fact[kind][]=&cashless_fact[location_id][]=&cashless_fact[machine_id][]=&cashless_fact[machine_type_id][]=&cashless_fact[masked_card_number]=&cashless_fact[occurred_at]=custom&cashless_fact[payment_authorizer_id][]=&cashless_fact[place]=&cashless_fact[point_of_sale]=&cashless_fact[product_type][]=&cashless_fact[request_number]=&cashless_fact[route_id][]=&cashless_fact[status][]=&cashless_fact[status][]={status}&format=xlsx&per_page=10000"')
    chrome.execute_script(f'window.location.href = "https://rodaconveniencia.amlabs.com.br/reportViewer.jsp?report=EntryInvoiceStatement&tscache=1633531093242&VENDORID=&SUPPLYID=&STOREROOMID=&CFOPID=&STATUS=Recebida&DHEMISTART=&DHEMIFINISH=&RECEIVINGDATESTART={day_begin}%2F{month_begin}%2F{year_begin}&RECEIVINGDATEFINISH={day_end}%2F{month_end}%2F{year_end}&CREATIONDATESTART=&CREATIONDATEFINISH=&ORDERBYOPTION=Compra&DRILLDOWN=true&xls=true"')

    n = 3
    dataframe = pandas.DataFrame.from_dict({})


    time.sleep(5.0)
    xlsdata = pandas.read_excel(os.path.join(localdir,  os.listdir(localdir)[0]), skiprows=n)
    dataframe = dataframe.append(xlsdata)

    
    csvfilename = f"{start_date.strftime('%d%m%Y%H%M%S')}-{end_date.strftime('%d%m%Y%H%M%S')}.csv"
    
    csvfilename = os.path.join(datasetdir, csvfilename)
    # dataframe.to_csv(csvfilename, sep=';')
    return dataframe



  def gerar_custos(self):
    
    try:
      df_custos1= pandas.read_csv(r"Custos.csv",",",index_col=0,low_memory=False)
      df_custos1['data de recebimento']=pandas.to_datetime(df_custos1['data de recebimento'])
      start_date = df_custos1['data de recebimento'].max()
      start_date=start_date.replace( hour=0,minute=0,second=0, microsecond=0)
      df_custos1 = df_custos1.loc[df_custos1['data de recebimento']<pandas.to_datetime(start_date)]

    except:
      start_date= datetime(2016,10,1)
      df_custos1=pandas.DataFrame()
    
    end_date = datetime.today() - timedelta(days=1)
    end_date=end_date.replace( hour=23,minute=59,second=59, microsecond=0)

    print(start_date)
    print(end_date)
    dfcustos = Tricks().pegar_infos(start_date=start_date,end_date=end_date)
    df = dfcustos[["nota fiscal",'data de emissão da nota fiscal',"data de recebimento",'código do fornecedor','fornecedor','código do produto','produto','quantidade','valor do produto (item)','valor do desconto (item)','valor das outras despesas (item)','ICMS ST (item)','FCP ST (item)','IPI (item)']]


    custofinal = (df["valor do produto (item)"]+df['IPI (item)']+df['FCP ST (item)']+df['ICMS ST (item)']+df['valor das outras despesas (item)']-df['valor do desconto (item)'])/df['quantidade']
    df['Custo Final']=custofinal
    

    df_final = pandas.concat([df,df_custos1],ignore_index=True)
    df_final = df_final.drop_duplicates()
    df_final.to_csv("Custos.csv",sep=",")
    return df_final


# a = Tricks().gerar_custos()
# %%
