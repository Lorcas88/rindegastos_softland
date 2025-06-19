import requests
import json
import os
from dotenv import load_dotenv
import re
from datetime import datetime, date
import pyodbc
import pandas as pd
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Cargar variables de entorno
load_dotenv()
required_vars = ["DB_SERVER", "DB_NAME", "DB_USER", "DB_PASSWORD", "TOKEN", "TABLE_MOVIM", "PROC_INSERT_CBTE", "PROC_INSERT_MOVS"]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    raise EnvironmentError(f"Faltan variables en el .env: {', '.join(missing_vars)}")

# Variables de entorno
server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
table_movim = os.getenv("TABLE_MOVIM")
proc_cbte = os.getenv("PROC_INSERT_CBTE")
proc_movs = os.getenv("PROC_INSERT_MOVS")
token = os.getenv("TOKEN")

# Cadena de conexión a BD
conn_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'

def integracion_reporte_rindegastos(id, cpb_num, headers):
    integration_api_base = "https://api.rindegastos.com/v1/setExpenseReportIntegration"
    payload = {
        "Id": id, # ID del reporte
        "IntegrationStatus": 1, # Integrado (1) o No Integrado (0)
        "IntegrationCode": cpb_num, # Código de comprobante contable
        "IntegrationDate": datetime.now().isoformat() # Fecha de integración
    }
    try:
        response = requests.put(integration_api_base, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al conectarse a la API de integración: {e}")
    except Exception as e:
        logging.error(f"Error al procesar la integración del reporte: {e}")
    return False

def obtener_informes(headers):
    url = "https://api.rindegastos.com/v1/getExpenseReports?IntegrationStatus=0&Status=1"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    # Se carga la respuesta de la url en un JSON,
    # para poder usar diccionario de Python. 
    return json.loads(response.text)

def procesar_informes(informes, cursor, headers):
    for informe in informes:
        try:
            id = str(informe["Id"])
            rut_empleado = informe["EmployeeIdentification"].split("-")[0].replace(".", "")
            if not rut_empleado:
                logging.warning(f"RUT vacío. Reporte {informe['ReportNumber']}, ID: {id}")
                continue
            digver_empleado = informe["EmployeeIdentification"].split("-")[1]
            nombre_empleado = informe["EmployeeName"]
            empresa = 'HAXIA' if rut_empleado == '77235100' else informe["ExtraFields"][0]["Value"]
            if informe["ExtraFields"][1]["Code"] == "FXR":
                if integracion_reporte_rindegastos(id, "", headers):
                    # Se realiza la integración para evitar procesar los datos y hacerlo de manera manual
                    print(f"Reporte pertenece a un Fondo. ID: {id}, N° reporte: {informe['ReportNumber']}")
                    continue
            politica = informe["PolicyName"]
            cuenta_contable_politica = "2-1-03-07-002" if politica.endswith("(CCH)") else "1-1-01-07-003"
            glosa_cbte = f"{informe['ExtraFields'][1]['Value'].upper()} N° {informe['ReportNumber']} {nombre_empleado.upper()}"[:60]
            fecha_cbte = date.fromisoformat(informe["SendDate"])
            total_cpbte = informe["ReportTotalApproved"]
            
            # Llamar al procedimiento almacenado y realizar commit de los cambios
            cursor.execute(f"USE {empresa}; EXEC {proc_cbte} ?, ?", glosa_cbte, fecha_cbte)
            cursor.nextset()
            resultado = cursor.fetchone() # Obtener el resultado
            if not resultado:
                raise ValueError("El procedimiento no retornó número de comprobante")
            cpb_num = resultado[0]
            logging.info("Se generó el comprobante con éxito")
            
            movimientos = obtener_movimientos(id, headers)
            datos = construir_datos_movimientos(movimientos, fecha_cbte, cpb_num, total_cpbte, glosa_cbte, rut_empleado, digver_empleado, nombre_empleado, cuenta_contable_politica)
            insertar_movimientos(cursor, empresa, datos)
            # Hacer commit de lo insertado en la base, ya que si llegó hasta acá el código, significa que no hubo conflicto con la data
            cursor.connection.commit()
            if integracion_reporte_rindegastos(id, cpb_num, headers):
                print(f"Integración realizada para reporte {informe['ReportNumber']}. Empresa: {empresa}, ID: {id}, Cpbte: {cpb_num}")
        except Exception as e:
            logging.error(f"Error procesando informe ID {informe['Id']}: {e}")
            cursor.connection.rollback()

def obtener_movimientos(id, headers):
    # Se realiza un request para obtener el detalle del reporte de gasto y 
    # poblar el cbte con el detalle de los movimientos    
    url = f"https://api.rindegastos.com/v1/getExpenses?OrderBy=3&ReportId={id}&Status=1&Order=ASC"
    response = requests.get(url, headers=headers)
    response.encoding = "utf-8"
    response.raise_for_status()
    return json.loads(response.text)["Expenses"]

def construir_datos_movimientos(movimientos, fecha_cbte, cpb_num, total_cpbte, glosa_cbte, rut_empleado, digver_empleado, nombre_empleado, cuenta_politica):
    datos = [] # Aquí se almacenan los datos de cada iteración
    for i, movimiento in enumerate(movimientos):
        numero_doc = ''.join(re.findall(r'\d+', movimiento["ExtraFields"][1]["Value"]))
        if movimiento["Supplier"] == "Uber SPA":
            numero_doc = datetime.strptime(movimiento["IssueDate"], "%Y-%m-%d").strftime("%Y%m%d")
        glosa = f"{movimiento['Note']} {movimiento['ExtraFields'][0]['Value'].split(' ')[0]} {numero_doc}"[:255]
        # Obtener el último día hábil del mes de la fecha dada
        fecha_mov = pd.to_datetime(movimiento["IssueDate"])
        ultimo_dia_habil = fecha_mov + pd.offsets.BMonthEnd(0)
        if movimiento["ExtraFields"][0]["Code"] == "FL":
            cuenta_contable = "2-1-03-01-001"
            centro_costo = "00000000"
            rut_sin_dv = movimiento["ExtraFields"][2]["Value"].split("-")[0]
            dig_ver = movimiento["ExtraFields"][2]["Value"].split("-")[1]
            supplier = movimiento["Supplier"]
            es_proveedor = "S"
            tipo_documento = "EF"
            num_documento = 1
            tipo_doc_ref = "FL"
            num_doc_ref = int(movimiento["ExtraFields"][1]["Value"])
        else:
            # Los movimientos que no son factura, no necesitan rut, ni tipo de pago
            cuenta_contable = movimiento["CategoryCode"]
            centro_costo = movimiento["ExtraFields"][3]["Code"]
            rut_sin_dv = "0000000000"
            dig_ver = "0"
            supplier = ""
            es_proveedor = "N"
            tipo_documento = "00"
            num_documento = 0
            tipo_doc_ref = "00"
            num_doc_ref = 0
        # Añadir la tupla a la lista de datos
        datos.append(
            (
                str(fecha_cbte.year), # Año
                str(cpb_num)[:8], # Nro cmpbte
                i, # Correlativo
                cuenta_contable[:18], # Cta Contable
                fecha_cbte, fecha_cbte.strftime("%m"), # fecha y mes de fecha
                centro_costo[:8], # Centro Costo
                rut_sin_dv[:10], dig_ver[:1], # RUT empresa factura
                supplier[:60], es_proveedor, #Proveedor
                tipo_documento, num_documento, # Tipo documento de trx y num
                fecha_mov.date(), ultimo_dia_habil.date(), # fecha factura y vencimiento
                tipo_doc_ref, num_doc_ref, # factura de ref y su numero
                movimiento["Total"], 0, glosa # Total y glosa como descripción 
            )
        )
    # Construir la línea para el haber. Tener en cuenta que los datos de estas líneas,
    # solo serán de personas naturales, ya que este dato es para contabilizar el reembolso de personas de la empresa.
    datos.append(
        (
            str(fecha_cbte.year), 
            str(cpb_num)[:8], 
            len(movimientos), # Correlativo final
            cuenta_politica[:18], # Cta Contable
            fecha_cbte, fecha_cbte.strftime("%m"), # fecha y mes de fecha
            "00000000", # Sin CCosto
            rut_empleado[:10], digver_empleado[:1], # RUT empleado
            nombre_empleado[:60], "N", # No es proveedor porque es el reembolso al empleado
            "00", 0, # Sin doc
            fecha_cbte, ultimo_dia_habil.date(), # fecha emision cbpte y vencimiento
            "00", 0, # tipo doc ref y su numero
            0, total_cpbte, glosa_cbte
        )
    )
    return datos

def insertar_movimientos(cursor, empresa, datos):
    cursor.fast_executemany = True
    cursor.execute(f"USE {empresa}; EXEC {proc_movs} ?", [datos])
    cursor.nextset()
    resultados = cursor.fetchall()
    for r in resultados:
        logging.info(f"Resultado procedimiento movimientos: {r[0]}")

def main():
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            logging.info("Conexión exitosa a SQL Server")
            headers = {'Authorization': f'Bearer {token}'}
            informes = obtener_informes(headers)["ExpenseReports"]
            if informes:
                procesar_informes(informes, cursor, headers)
            else:
                logging.info("No hay informes pendientes de integración.")
    except pyodbc.Error as e:
        logging.error(f"Error de conexión a SQL Server: {e}")
    except Exception as e:
        logging.error(f"Error general: {e}")

if __name__ == "__main__":
    main()