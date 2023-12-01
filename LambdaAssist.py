import json
import boto3
import logging
from botocore.exceptions import ClientError
from decimal import Decimal
import uuid
from datetime import datetime

# Obtener la fecha y hora actuales
current_datetime = datetime.now()

# Formatear la fecha y hora como una cadena
formatted_date = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

# Generar un UUID
new_id = str(uuid.uuid4())
logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('dynamodb')
dynamo = boto3.resource('dynamodb').Table('transactional')
account = boto3.resource('dynamodb').Table('bankAccounts')

def lambda_handler(event, context):
    logger.info(">>>>>>>>>>>>>>Conexión a DynamoDB establecida correctamente.")
    body = None
    status_code = 200
    headers = {"Content-Type": "application/json"}
    try:
        route_key = event['routeKey']
        path_parameters = event.get('pathParameters', {})
        body = None
        
        if route_key == 'POST /transaction':
            request_json = json.loads(event.get('body', '{}'))
    
            logger.info(f">>>>>>>>>>>>>>Iniciando Transaccion {request_json}")
            # Generar un nuevo ID para la transacción
           
             # Agregar un nuevo campo "fecha" con la fecha actual formateada
            validate_transaction(request_json)
            logger.info("********************** paso validacion")
            discount_amount(request_json)
            add_amount(request_json)
            logger.info("********************** se movio el saldo de origen a destino")
            dynamo.put_item(
                Item={
                    "id_trx": new_id,
                    "customerOrigin": request_json["customerOrigin"],
                    "customerDestination": request_json["customerDestination"],
                    "amount": request_json["amount"],
                    "time": formatted_date,
                    "status": 2,
                }
            )
            body = f'Post item {request_json.get("id")}'

        else:
            logger.info(">>>>>>>>>>>>>>Ruta no establecida ")
            raise Exception(f"Unsupported route: {event['routeKey']}")
    except ClientError as e:
        status_code = 400
        body = e.response["Error"]["Message"]
        logger.error(f"Error al interactuar con DynamoDB: {e}")
    finally:
        body = json.dumps(body)

    return {
        "statusCode": status_code,
        "body": body,
        "headers": headers,
    }


def validate_transaction(request_json):
    def check_account(account_data, account_type):
        if not account_data:
            raise Exception(f"Cuenta de {account_type} no encontrada")
        elif account_data.get('status') != 1:
            raise Exception(f"Cuenta de {account_type} inactiva")
        elif Decimal(account_data.get('amount', 0)) < request_json.get('amount'):
            raise Exception(f"Saldo insuficiente en la cuenta de {account_type}")

    origin = account.get_item(Key={'id': request_json["customerOrigin"]}).get('Item', {})
    check_account(origin, "origen")

    destination = account.get_item(Key={'id': request_json["customerDestination"]}).get('Item', {})
    check_account(destination, "destino")
        
    return True

    
def discount_amount(request_json):
    origin_old = account.get_item(Key={'id': request_json["customerOrigin"]}).get('Item', {})
    new_amount = Decimal(origin_old.get('amount', 0)) - request_json['amount']
    account.put_item(
                Item={
                    'id': origin_old.get('id'),
                    'holder': origin_old.get('holder'),
                    'amount': new_amount,
                    'dateCreated': origin_old.get('dateCreated'),
                    'status': origin_old.get('status'),
                }
            )
    return None
    
def add_amount(request_json):
    destination_old = account.get_item(Key={'id': request_json["customerDestination"]}).get('Item', {})
    new_amount = Decimal(destination_old.get('amount', 0)) + request_json['amount']
    account.put_item(
                Item={
                    'id': destination_old.get('id'),
                    'holder': destination_old.get('holder'),
                    'amount': new_amount,
                    'dateCreated': destination_old.get('dateCreated'),
                    'status': destination_old.get('status'),
                }
            )
    return None