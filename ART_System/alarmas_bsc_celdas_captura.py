import pandas as pd
from sqlalchemy import create_engine,types
import datetime as dt
import numpy as np

def funcion():
    engine = create_engine('mysql+mysqldb://E09943:w0rk3r@172.31.158.5/nsn_bsc_registros')
    engine_insert = engine = create_engine('mysql+mysqldb://E09943:w0rk3r@172.31.158.5/nsn_reg_alarmas')
    data = pd.read_sql("SELECT * FROM nsn_bsc_registros.registro_alarma_celdas",engine)
    data['fecha'] = pd.to_datetime(data["FECHA"].astype('str')+' '+data["HORA"].apply(lambda x: '{:02d}:{:02d}:{:02d}'.format(x.components.hours, x.components.minutes, x.components.seconds)))
    data['fecha_captura'] = pd.to_datetime(data["FECHA_CAPTURA"].astype('str')+' '+data["HORA_CAPTURA"].apply(lambda x: '{:02d}:{:02d}:{:02d}'.format(x.components.hours, x.components.minutes, x.components.seconds)))
    data = data.pivot_table(index=['FUENTE', 'NUMERO_BCF', 'NUMERO_BTS', 'NOMBRE_CELDA','NUMERO_ALARMA','CODIGO_ALARMA'], columns='TIPO',
                                   values=['fecha','fecha_captura'],aggfunc='first')
    data.columns = data.columns.map('{0[1]}_{0[0]}'.format)
    data.reset_index(inplace=True)
    data.loc[pd.notnull(data['NOTICE_fecha']),['ALARM_fecha','CANCEL_fecha']]=data['NOTICE_fecha']
    data.loc[pd.notnull(data['DISTUR_fecha']),['ALARM_fecha','CANCEL_fecha']]=data['DISTUR_fecha']
    data.loc[pd.notnull(data['NOTICE_fecha_captura']),'CANCEL_fecha_captura']=data['NOTICE_fecha_captura']
    data.loc[pd.notnull(data['DISTUR_fecha_captura']),'CANCEL_fecha_captura']=data['DISTUR_fecha_captura']
    now = dt.datetime.now()
    data['status']=np.nan
    data['sistema']='HISTORICO'
    data.loc[pd.isnull(data['CANCEL_fecha']),'status']='OOS'
    data.loc[pd.isnull(data['CANCEL_fecha']),'sistema']='ONLINE'
    data.fillna(value={
        'ALARM_fecha':pd.to_datetime("{}-{}-{} 00:00:00".format(now.year,now.month, now.day)),
        'CANCEL_fecha':pd.to_datetime("{}-{}-{} {:02d}:{:02d}:{:02d}".format(now.year,now.month, now.day,now.hour,now.minute,now.second))
        },inplace=True
               )
    data['fecha_hoy'] = pd.to_datetime("{}-{}-{} 00:00:00".format(now.year,now.month, now.day))
    data['fecha_comp']=data['ALARM_fecha']
    data.loc[data.ALARM_fecha<data.fecha_hoy, 'fecha_comp']=data['fecha_hoy']
    data['tiempo']=((data['CANCEL_fecha']-data['fecha_comp']).dt.total_seconds())/60
    data.loc[pd.isnull(data.CANCEL_fecha_captura), 'CANCEL_fecha_captura']=data['ALARM_fecha_captura']
    data['fecha']=data['CANCEL_fecha_captura'].dt.strftime('%Y-%m-%d')
    data['hora']=data['CANCEL_fecha_captura'].dt.strftime('%H:%M:%S')
    data.rename(index=str, columns={"FUENTE": "bsc", 
                                    "NUMERO_BCF": "numero_bcf",
                                    "NUMERO_BTS":"numero_sec",
                                    "NOMBRE_CELDA":"nombre_sec",
                                    "ALARM_fecha":"hora_inicio",
                                    "CANCEL_fecha":"hora_fin"

                                   },inplace=True)
    data.drop(axis=1,labels=["NUMERO_ALARMA","ALARM_fecha_captura","CANCEL_fecha_captura","fecha_hoy","fecha_comp","NOTICE_fecha_captura","DISTUR_fecha_captura","NOTICE_fecha","DISTUR_fecha"],inplace=True)
    data['num']=data.index
    data.set_index(keys='num')
    data = data[data.columns.tolist()[-1:] + data.columns.tolist()[:-1]]
    data.to_sql(name="registro_alarma",schema="nsn_reg_alarmas",con=engine_insert,if_exists="replace", index=False)
    return data

if __name__=='__main__':
    data = funcion()
