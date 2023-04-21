import pandas as pd
from telegram.ext import Updater, CommandHandler, CallbackContext
from datetime import datetime
from credenciales import token,key, gc, key_query
import logging

# Configurar el registro de errores
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.ERROR)

def error(update, context):
    """Manejar las excepciones"""
    logging.error(f'Error: {context.error}')
    update.message.reply_text('Hubo un error. Pero sigo trabajando')

# Agregar el manejador de errores
contexto = CallbackContext
def actualizar(contexto):
  global BD  
  global ASISTENCIA
  global ina
  global hoy 
  global faltas 
  global ina
  global per
  global rep
  global ren
  global VACANTES
  global anti_join
  global INCIDENCIAS
  global rows
  #global FIN_SUP
  global FIN_OP
  global duplicates 
  global nuevo
  global anti_join2
  global ASISTENCIA_NEW
  global BD_NEW
  # fecha de hoy
  dia = datetime.today()
  hoy = dia.strftime('%Y-%m-%d')

  # ASISTENCIA OPERARIOS
  rows = gc.open_by_key(key).worksheet("ASISTENCIA").get_all_records()
  ASISTENCIA = pd.DataFrame(rows)
  ASISTENCIA['fecha'] = pd.to_datetime(ASISTENCIA['fecha'], format='%d/%m/%Y')
  ASISTENCIA_NEW = ASISTENCIA.loc[:,['fecha','Operario','Regional','Agencia','Asistencia']]  
  # DF_HOY Muestra la carga de asistencia del dia 
  DF_HOY = ASISTENCIA[ASISTENCIA['fecha'] == hoy]

  #DUPLICADOS
  duplicates = ASISTENCIA[ASISTENCIA['fecha'] == hoy]['Operario'].value_counts()[lambda x: x>1]
  duplicates = duplicates.to_frame().reset_index().rename(columns={'index': 'Operario', 'Operario': 'Cantidad'})
 
   # INCIDENCIAS Muestra las Incidencias diarias (Reposos, Permisos, Inasistencias)
  INCIDENCIAS = DF_HOY[DF_HOY['Asistencia'] != 'ASISTENTE' ]
  faltas = INCIDENCIAS.loc[: ,["Operario","Cedula","Regional","Agencia","Asistencia","Novedad"] ]
  faltas = faltas.sort_values(['Regional','Operario'])
  # Todas las inasistencias
  ina = faltas[faltas['Asistencia'] == 'INASISTENTE']
  # Todas las faltas justificadas
  per = faltas[faltas['Asistencia'] == 'PERMISO']
  # todos los reposos
  rep = faltas[faltas['Asistencia'] == 'REPOSO']
  #vacaciones
  ren = faltas[faltas['Asistencia'] == 'RENUNCIA']
    
  # BD BASE DE DATOS  
  BD = gc.open_by_key(key).worksheet("BD").get_all_records()
  BD = pd.DataFrame(BD)
  BD_NEW = BD.loc[:,['ID','OPERARIO','CEDULA','REGIONAL', 'AGENCIA','COORDINADOR','SUPERVISOR', 'FECHA INGRESO']]

  # ACTUALIZAR CAMBIOS EN LA BD
  query = gc.open_by_key(key_query).worksheet("Query operarios")
  query = query.get_all_records()
  query = pd.DataFrame(query)
  left_join = pd.merge(query, BD, how='left', left_on='CEDULA', right_on='CEDULA')
  nuevo = left_join.loc[:,['ID_x', 'OPERARIO_x', 'CEDULA', 'ESTATUS_x', 'NOMBRE DE LA AGENCIA_x','DIRECCIÓN DE LA AGENCIA', 'REGION_x', 'REGIONAL_x', 'ESTADO_x','MUNICIPIO_x', 'CODIGO', 'COORDINADOR_x', 'SUPERVISOR_x', 'TELEFONO_x','FECHA DE INGRESO','FECHA DE NACIMIENTO_y','DIRECCIÓN DE VIVIENDA_y', 'FOTO_y']]      
  
  # VACANTES
  VACANTES = BD.loc[: ,['ID','OPERARIO','REGIONAL','AGENCIA']]
  VACANTES = VACANTES.sort_values(['REGIONAL','OPERARIO'])
  VACANTES = VACANTES[VACANTES['OPERARIO'] == 'VACANTE']
  VACANTES = VACANTES.loc[:,['ID','REGIONAL','AGENCIA']]  
  
  # FILTRADO DE COLUMNAS DE LA BD
  BD = BD.loc[:,['COORDINADOR','OPERARIO','REGIONAL','AGENCIA']]

  # FALTANTES POR CARGAR
  outer = pd.merge(BD, DF_HOY, left_on='OPERARIO', right_on='Operario',how='outer', indicator=True)
  anti_join = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
  anti_join = anti_join[(anti_join.OPERARIO != 'INACTIVA') & (anti_join.OPERARIO != 'VACANTE')]
  anti_join = anti_join.loc[:,['COORDINADOR','REGIONAL','AGENCIA','OPERARIO']]
  anti_join = anti_join.sort_values(['REGIONAL','OPERARIO'])
  anti_join2 = anti_join.loc[:,['REGIONAL','OPERARIO']].groupby('REGIONAL').count().reset_index()

  # FIN DE SEMANA OPERARIOS
  FIN_OP = gc.open_by_key(key).worksheet("FIN").get_all_records()    
  FIN_OP = pd.DataFrame(FIN_OP)
  FIN_OP['FECHA'] = pd.to_datetime(FIN_OP['FECHA'], format='%d/%m/%Y')
  FIN_OP = FIN_OP[FIN_OP['FECHA'] == hoy]
  FIN_OP = FIN_OP.loc[:,['OPERARIO', 'REGIONAL','ASISTENCIA']]

  # FIN DE SEMANA SUPERVISORES
"""  FIN_SUP = gc.open_by_key(key).worksheet("FIN_SEMANA").get_all_records()    
  FIN_SUP = pd.DataFrame(FIN_SUP)
  FIN_SUP['FECHA'] = pd.to_datetime(FIN_SUP['FECHA'], format='%d/%m/%Y')
  FIN_SUP = FIN_SUP[FIN_SUP['FECHA'] == hoy]
  FIN_SUP = FIN_SUP.loc[:,['SUPERVISOR','REGIONAL', 'ASISTENCIA']]"""

"""  # EVALUACIONES
  form = gc.open_by_key(key).worksheet("Respuestas de formulario 1").get_all_records()
  evaluaciones = pd.DataFrame(form)
  evaluaciones['Fecha'] = pd.to_datetime(evaluaciones['Fecha'], format='%d/%m/%Y')
  evealuaciones_daily = evaluaciones[evaluaciones["Fecha"] == hoy]
  evaluaciones_daily = evealuaciones_daily.loc[:,['Fecha','SUPERVISOR','REGIONAL','AGENCIA O PISO DE TORRE','OPERARIO','EVALUACION DEL OPERARIO [PASILLOS]','EVALUACION DEL OPERARIO [OFICINAS Y MOBILIARIO]','EVALUACION DEL OPERARIO [VIDRIOS]','EVALUACION DEL OPERARIO [BAÑOS]','EVALUACION DEL OPERARIO [COMEDOR]']]
  renombre = {'Fecha' : 'FECHA', 'AGENCIA O PISO DE TORRE' : 'AGENCIA', 'EVALUACION DEL OPERARIO [PASILLOS]' : 'PASILLOS','EVALUACION DEL OPERARIO [OFICINAS Y MOBILIARIO]': 'OFICINAS', 'EVALUACION DEL OPERARIO [VIDRIOS]' : 'VIDRIOS', 'EVALUACION DEL OPERARIO [BAÑOS]' : 'BAÑOS', 'EVALUACION DEL OPERARIO [COMEDOR]' : 'COMEDOR'}
  evaluaciones_update = evaluaciones_daily.rename(columns=renombre)
  numeros = ['PASILLOS','OFICINAS', 'VIDRIOS','BAÑOS','COMEDOR']
  for i in numeros:
    evaluaciones_update[i] = pd.to_numeric(evaluaciones_update[i])
  promedio = evaluaciones_update[numeros]
  evaluaciones_update['promedio'] = promedio.mean(axis=1)/4
  todos = evaluaciones_update.loc[:,['OPERARIO','REGIONAL','AGENCIA', 'promedio']].sort_values(['REGIONAL','OPERARIO'])
  malos = todos[todos['promedio'] <= 0.5].sort_values(['REGIONAL','OPERARIO'])
  malos['promedio'] = malos['promedio'].apply(lambda x: '{:.2f}%'.format(x * 100))
  regulares = todos[(todos['promedio'] > 0.5) & (todos['promedio'] <= 0.8)].sort_values(['REGIONAL','OPERARIO'])
  regulares['promedio'] = regulares['promedio'].apply(lambda x: '{:.2f}%'.format(x * 100))
  buenos = todos[(todos['promedio'] > 0.8) & (todos['promedio'] <= 0.95)].sort_values(['REGIONAL','OPERARIO'])
  buenos['promedio'] = buenos['promedio'].apply(lambda x: '{:.2f}%'.format(x * 100))
  excelentes = todos[todos['promedio'] == 1].sort_values(['REGIONAL','OPERARIO'])
  excelentes['promedio'] = excelentes['promedio'].apply(lambda x: '{:.2f}%'.format(x * 100))
  todos['promedio'] = todos['promedio'].apply(lambda x: '{:.2f}%'.format(x * 100))"""


def start(update, context):
    chat_id = update.message.chat_id
    photo_path = "slogan.png" # Ruta de la imagen que quieres enviar
    photo = open(photo_path, 'rb')
    context.bot.send_photo(chat_id=chat_id, photo=photo, caption= '<b>Menu de opciones 📣</b>\n\n'\
        'Selecciona cualquier opcion para Consultar:\n\n'\
        '1. ⭕️ /Incidencias Inasistencias, Reposos, Permisos\n\n'\
        '2. ❎ /Faltantes por cargar del dia de hoy \n\n'\
        #'3. 📈 /Evaluaciones del operario\n\n'\
        '3. 📅 /Fin_semana de los Supervisores y Operarios\n\n'\
        '4. 👥 /Duplicados del dia de hoy \n\n'\
        '5. ⚠️ /Vacantes disponibles\n\n'\
        '6. 📊 /Dashboard Muestra el Dashboard de los operarios \n\n\n\n'
        '7. 📨 /Archivo_bolsas Se crea un archivo de operarios a recibir Bolsas\n\n'\
        '8. 🚩 /Actualizar_BD (NO EJECUTAR )Solo usar para actualizar la BD de la APP', parse_mode='HTML')

def Actualizar_BD(update, context):
  global nuevo
  # Autenticar la conexión con las credenciales proporcionadas
    # Obtener la hoja de cálculo correspondiente
  valores = nuevo.values.tolist()
  destino = gc.open('BD NACIONAL OPERARIOS').worksheet('BD')
  rango_destino = 'A2:R370'
  for fila in valores:
      for i, valor in enumerate(fila):
          if isinstance(valor, (int, float)):
              fila[i] = str(valor)
  destino.update(rango_destino, valores)
  context.bot.send_message(chat_id=update.effective_chat.id, text='Los datos se han Actualizado exitosamente ✅')
  

def Fin_semana(update, context):
  VER = 'Muestra la asistencia de los Supervisores y Operarios el fin de semana:\n\n' + '1. ✅ /Fin_supervisores Muestra los Supervisores cargados hoy\n\n\n' + '2. ✅ /Fin_Operarios Muestra los Operarios Cargados hoy \n\n'
  update.message.reply_text(VER)

def Fin_supervisores(update, context):
  global FIN_SUP
  if FIN_SUP.empty == False:
      for i in range(len(FIN_SUP)):
          titulo1 = f"Supervisores cargados hoy {hoy} ✅\n" 
          INCIDENCIAS1= titulo1 + '\n SUPERVISOR: ' + str((FIN_SUP['SUPERVISOR'].iloc[i])) +' está ' + str((FIN_SUP['ASISTENCIA'].iloc[i]))
          update.message.reply_text(INCIDENCIAS1)
  else:
    no = f"Hoy {hoy} no hay supervisores trabajando un fin de semana"
    update.message.reply_text(no)


def Fin_operarios(update, context):
  global FIN_OP
  if FIN_OP.empty == False:
      for i in range(len(FIN_OP)):
          titulo1 = f"Operarios cargados hoy {hoy} ✅\n" 
          INCIDENCIAS1= titulo1 + '\n Operario: ' + str((FIN_OP['NOMBRE'].iloc[i])) +'\n Status: ' + str((FIN_OP['ASISTENCIA'].iloc[i]))
          update.message.reply_text(INCIDENCIAS1)
  else:
    no = f" Primero hoy debe ser Fin de semana si no, Hoy {hoy} No hay Operarios trabajando "
    update.message.reply_text(no)

def Incidencias(update, context):
  VER = 'Que tipo de incidencias quieres ver:\n\n' + '1. 🔴 /Inasistencias Muestra las faltas Injustificadas\n\n' + '2. 🟠 /Permisos Otorgados por coordinadores \n\n' + '3. 🟡 /Reposos Muestra Operarios de Reposos \n\n' + '3. ❕ /Renuncias Muestra Operarios de Vacaciones \n\n'+ '4. 🚦 /Todas Muestra todas las Incidencias del dia \n\n' + '5. ↩️ /start Presione Para Volver al MENU principal \n\n'
  update.message.reply_text(VER)

def Inasistencias(update, context):
  global ina
  if ina.empty == False:
      for i in range(len(ina)):
          titulo1 = f"Inasistencias del Día {hoy}" 
          INCIDENCIAS1= titulo1 + '\n Operario: ' + str((ina['Operario'].iloc[i])) +'\n Regional: ' + str((ina['Regional'].iloc[i])) +'\n Agencia: ' + str((ina['Agencia'].iloc[i])) + '\n Tipo de falta: ' + '🔴 ' + str((ina['Asistencia'].iloc[i])) + '\n Novedad: ' + str((ina['Novedad'].iloc[i]))
          update.message.reply_text(INCIDENCIAS1)
  else:
    no = f"✅ Personal sin inasistencias del día {hoy}"
    update.message.reply_text(no)

def Permisos(update, context):
  global per
  if per.empty == False:
    for i in range(len(per)):
      titulo1 = f"Permisos del Día {hoy} 📣 " 
      INCIDENCIAS2= titulo1 + '\n Operario: ' + str((per['Operario'].iloc[i])) +'\n Regional: ' + str((per['Regional'].iloc[i])) +'\n Agencia: ' + str((per['Agencia'].iloc[i])) + '\n Tipo de falta: '  + '🟠 '+ str((per['Asistencia'].iloc[i])) + '\n Novedad: ' + str((per['Novedad'].iloc[i]))
      update.message.reply_text(INCIDENCIAS2)
  else:
    no = f"Personal sin permisos del día {hoy} ✅"
    update.message.reply_text(no)
     
def Reposos(update, context):
  global rep
  if rep.empty == False:
    for i in range(len(rep)):
      titulo1 = f"Reposos del Día {hoy}" 
      INCIDENCIAS3= titulo1 + '\n Operario: ' + str((rep['Operario'].iloc[i])) +'\n Regional: ' + str((rep['Regional'].iloc[i])) +'\n Agencia: ' + str((rep['Agencia'].iloc[i])) + '\n Tipo de falta: ' + '🟡 ' + str((rep['Asistencia'].iloc[i])) + '\n Novedad: ' + str((rep['Novedad'].iloc[i]))
      update.message.reply_text(INCIDENCIAS3)
  else:
    no = f"Personal sin Reposos del día {hoy} ✅"
    update.message.reply_text(no)

def Renuncias(update, context):
  global ren
  if ren.empty == False:
    for i in range(len(ren)):
      titulo1 = f"Renuncias del Día {hoy}" 
      INCIDENCIAS4= titulo1 + '\n Operario: ' + str((ren['Operario'].iloc[i])) +'\n Regional: ' + str((ren['Regional'].iloc[i])) +'\n Agencia: ' + str((ren['Agencia'].iloc[i])) + '\n Tipo de falta: ' + '❕ ' + str((ren['Asistencia'].iloc[i]))
      update.message.reply_text(INCIDENCIAS4)
  else:
    no = f"Personal sin Reposos del día {hoy} ✅"
    update.message.reply_text(no)

def Todas(update, context):
  global faltas
  if faltas.empty == False:
    for i in range(len(faltas)):
      titulo1 = f"Incidencias del Día {hoy}" 
      INCIDENCIAS= titulo1 + '\n Operario: ' + str((faltas['Operario'].iloc[i])) +'\n Regional: ' + str((faltas['Regional'].iloc[i])) +'\n Agencia: ' + str((faltas['Agencia'].iloc[i])) + '\n Tipo de falta: ' + str((faltas['Asistencia'].iloc[i])) + '\n Novedad: ' + str((faltas['Novedad'].iloc[i]))
      update.message.reply_text(INCIDENCIAS)
  else:
    no = f"Personal del día {hoy} completo ✅"
    update.message.reply_text(no)

def Faltantes(update, context):
  foul = 'Ver el nombre de los operarios faltantes o el numero de faltantes:\n\n' + '1. 🔤 /Nombres de los operarios faltantes por cargar hoy \n\n\n' + '2. 🔢 /Numeros de Operarios faltantes por cargar hoy \n\n'
  update.message.reply_text(foul)

    
def Nombres(update, context):
  global anti_join
  if anti_join.empty == False:
    for i in range(len(anti_join)):
      titulo2 = f" Operarios faltantes por Cargar hoy {hoy} ❌"
      FALTANTES =titulo2 + '\n Operario: ' + str((anti_join['OPERARIO'].iloc[i])) + '\n Regional: ' + str((anti_join['REGIONAL'].iloc[i])) + '\n Agencia: ' + str((anti_join['AGENCIA'].iloc[i]))
      update.message.reply_text(FALTANTES)
  else:
    empty = f"Todos los operarios de {hoy} fueron cargados exitosamente ✅"
    update.message.reply_text(empty)

def Numeros(update, context):
  global anti_join2
  if anti_join2.empty == False:
    for i in range(len(anti_join2)):
      titulo2 = f"Numero de operarios por cargar en la APP {hoy} ❌\n"
      FALTANTES =titulo2  + '\n Regional: ' + str((anti_join2['REGIONAL'].iloc[i])) + '\n Cantidad de operarios faltantes: ' +  str((anti_join2['OPERARIO'].iloc[i])) 
      update.message.reply_text(FALTANTES)
  else:
    empty = f"Todos los operarios de {hoy} fueron cargados exitosamente ✅"
    update.message.reply_text(empty)

def Duplicados(update, context):
  global duplicates
  if duplicates.empty == False:
      for i in range(len(duplicates)):
          titulo1 = f"Duplicados del Día {hoy}" 
          INCIDENCIAS1= titulo1 + '\n Operario: ' + str((duplicates['Operario'].iloc[i])) +'\n Veces cargado: ' + str((duplicates['Cantidad'].iloc[i]))
          update.message.reply_text(INCIDENCIAS1)
  else:
    no = f"Sin duplicados del dia {hoy} ✅"
    update.message.reply_text(no)

def Vacantes(update, contex):
  global VACANTES
  if VACANTES.empty == False:
    for i in range(len(VACANTES)):
      titulo3 = f"Vacantes actuales al dia {hoy} ⚠️"
      por_ingreso =titulo3 + '\n ID: ' + str((VACANTES['ID'].iloc[i])) + '\n REGIONAL: ' + str((VACANTES['REGIONAL'].iloc[i]))  + '\n AGENCIA : ' + str((VACANTES['AGENCIA'].iloc[i])) 
      update.message.reply_text(por_ingreso)
  else:
    no_vacantes = f"Al dia {hoy} no hay vacantes Disponibles ✅"
    update.message.reply_text(no_vacantes)

def Dashboard(update, context):
  URL = 'Aqui Podras visualizar del Dashboard de Gestion de los operarios 📊 \n\n' + 'https://lookerstudio.google.com/reporting/def9c71a-2027-46c9-851e-0b11e8223177/page/mjKDD'
  update.message.reply_text(URL)

def Archivo_bolsas(update, context):
  context.bot.send_message(chat_id=update.effective_chat.id, text='<b>ATENCION Para generar el archivo de las bolsas debes seguir los siguientes pasos:📣</b>\n\n'+
        '1.  Vas a escribir el comando \Bolsas seguido de 3 argumentos con espacios Los argumentos son:\n\n'+
        '2. fecha de inicio, fecha final y fecha de tolerancia\n\n'+
        '3.  "fecha de inicio" es el primer dia a tomar en cuenta para las bolsas de dicho mes\n\n'+
        '4.  "fecha final" es el ultimo dia a tomar en cuenta para las bolsas de dicho mes\n\n'+
        '5.  "fecha de tolerancia" es el dia maximo a tomar en cuenta de un operario nuevo ingreso es decir: \n\n'+
        '5.1 Si las bolsas de marzo se entregan el 12 de abril la tolerancia es que ese operario debio ingresar antes del 11 de marzo para recibir bolsa\n\n'+
        '6.  Sabiendo Esto entonces debemos tomar en cuenta que las fechas se escriben en un formato donde comienza por el año-mes-dia\n\n'+
        '7.  Quedaria de esta forma YYYY-MM-DD \n\n'+
        '8. Aqui tienes un ejemplo de como generar el archivo de las bolsas sabiendo todo esto ejemplificando con el mes de marzo:\n\n\n'+
        '/Bolsas 2023-03-01 2023-03-31 2023-03-10\n\n\n'
        '<b>NOTA: Esto generará un archivo en GOOGLE SHEET con las personas que reciben bolsas en ese mes en la hoja de calculo llamada BOLSAS separando cada pestaña de la hoja por cada capital</b>', parse_mode='HTML')

  
def Bolsas(update, context):
  global ASISTENCIA_NEW
  global BD_NEW
  BD_NEW['FECHA INGRESO'] = pd.to_datetime(BD_NEW['FECHA INGRESO'], format='%d/%m/%Y')
  inicio = context.args[0]
  final = context.args[1]
  fecha_ingreso = context.args[2]
  inicio = datetime.strptime(inicio, '%Y-%m-%d')
  final = datetime.strptime(final, '%Y-%m-%d')
  fecha_ingreso = datetime.strptime(fecha_ingreso, '%Y-%m-%d')
  data_mes = ASISTENCIA_NEW[(ASISTENCIA_NEW['fecha'].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d'), '%Y-%m-%d')) >= inicio) & (ASISTENCIA_NEW['fecha'].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d'), '%Y-%m-%d')) <= final)]
  asistencia = data_mes.groupby(['Operario', 'Asistencia']).size().unstack(fill_value=0)
  asistencia = asistencia.reset_index()
  noreciben_bolsa = asistencia[asistencia['INASISTENTE']  >=3] 
  noreciben_bolsa = noreciben_bolsa.loc[:,['Operario','ASISTENTE','INASISTENTE']]
  # hoja donde se almacenaran las datas de las bolsas
  hoja_calculo = gc.open('BOLSAS NACIONALES')
  # Limpiar las hoja de cálculo
  limpiar = ["REGIONALES", "NO RECIBEN"]
  for limpiando in limpiar:
    hoja = hoja_calculo.worksheet(limpiando)
    hoja.clear()
  # rellenar una vez limpia
  datos = [noreciben_bolsa.columns.tolist()] + noreciben_bolsa.values.tolist()
  hoja_calculo.worksheet("NO RECIBEN").update('A1', datos)
 # enviar no reciben bolsas 
  base = BD_NEW[BD_NEW['FECHA INGRESO'] <= fecha_ingreso]
  outer = pd.merge(base, noreciben_bolsa, left_on='OPERARIO', right_on='Operario',how='outer', indicator=True)
  bolsa = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
  bolsa = bolsa.loc[:,['OPERARIO','CEDULA','REGIONAL','COORDINADOR','SUPERVISOR']]
  df = bolsa[(bolsa["OPERARIO"] != "INACTIVA") & (bolsa["OPERARIO"] != "VACANTE")].sort_values(['REGIONAL', 'CEDULA'])
  datos2 = [df.columns.tolist()] + df.values.tolist()
  hoja_calculo.worksheet("REGIONALES").update('A1', datos2)
  #diccionario vacio
  groups = {}
# Iteramos sobre los grupos y agregamos los datos correspondientes al DataFrame correspondiente
  regionales = df['REGIONAL'].unique()
  for group in regionales:
      group_df = df[df['REGIONAL'] == group]
      groups[group] = group_df
# hoja a eliminar
  eliminar = ["ARAGUA", "CENTRO OCCIDENTE", "LOS ANDES", "OCCIDENTE", "ORIENTE CENTRO", "ORIENTE NORTE", "ORIENTE NORTE 2","ORIENTE SUR","CARABOBO"]

  # Eliminamos cada hoja de cálculo existente excepto la que se desea conservar
  for hoja_nombre in eliminar:
      hoja = hoja_calculo.worksheet(hoja_nombre)
      hoja_calculo.del_worksheet(hoja)
  # Creamos una hoja de cálculo para cada DataFrame y agregamos los datos a cada hoja
  for group, group_df in groups.items():
      worksheet = hoja_calculo.add_worksheet(title=group, rows=len(group_df), cols=len(group_df.columns))
      cell_list = worksheet.range(1, 1, len(group_df)+1, len(group_df.columns))
      for cell in cell_list:
          if cell.row == 1:
              cell.value = group_df.columns[cell.col - 1]
          else:
              cell.value = group_df.iloc[cell.row - 2, cell.col - 1]
      worksheet.update_cells(cell_list)
  longitud = len(df)
  context.bot.send_message(chat_id=update.effective_chat.id, text=f'Archivo-Bolsa de Capitales generado correctamente para {longitud} operarios ✅\n\n' + 'Podras visualizarlo aqui:\n\n' + 'https://docs.google.com/spreadsheets/d/1s5uB8JJN6lJVzrREETWmTinwC3NkCBdABeTVTYg_1vA/edit?usp=sharing')
  

if __name__=='__main__':
  updater = Updater(token, use_context=True)
  dp = updater.dispatcher
  dp.add_handler(CommandHandler('start', start))
  dp.add_handler(CommandHandler('Incidencias', Incidencias))
  dp.add_handler(CommandHandler('Inasistencias', Inasistencias))
  dp.add_handler(CommandHandler('Reposos', Reposos))
  dp.add_handler(CommandHandler('Permisos', Permisos))
  dp.add_handler(CommandHandler('Todas', Todas))
  dp.add_handler(CommandHandler('Renuncias', Renuncias))
  dp.add_handler(CommandHandler('Faltantes', Faltantes))
  dp.add_handler(CommandHandler('Numeros', Numeros))
  dp.add_handler(CommandHandler('Nombres', Nombres))
  dp.add_handler(CommandHandler('Duplicados', Duplicados))
  dp.add_handler(CommandHandler('Fin_semana', Fin_semana))
  dp.add_handler(CommandHandler('Fin_supervisores', Fin_supervisores))
  dp.add_handler(CommandHandler('Fin_operarios', Fin_operarios))
  dp.add_handler(CommandHandler('Vacantes', Vacantes))
  dp.add_handler(CommandHandler('Actualizar_BD', Actualizar_BD))
  dp.add_handler(CommandHandler('Dashboard', Dashboard))
  dp.add_handler(CommandHandler('Archivo_bolsas', Archivo_bolsas))
  dp.add_handler(CommandHandler('Bolsas', Bolsas, pass_args=True))
  dp.add_error_handler(error)
  actualizar(contexto)
  job_queue = updater.job_queue
  job_queue.run_repeating(actualizar, interval=300, first=0)
  updater.start_polling()
  print('Estoy Corriendo cada 5 minutos')
  updater.idle()