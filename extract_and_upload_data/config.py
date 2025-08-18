import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_DBNAME = os.getenv("SUPABASE_DBNAME")
DB_URL = (
    f"postgresql://{SUPABASE_USER}:{SUPABASE_PASSWORD}@"
    f"{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DBNAME}"
    )
DB_ENGINE = create_engine(DB_URL)
MS_CLIENT_ID = os.getenv('MS_CLIENT_ID')
MS_TENANT_ID = 'consumers'
AUTHORITY = f"https://login.microsoftonline.com/{MS_TENANT_ID}"
SCOPES = ["Mail.Read"]
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0/me/messages"
SENDER_EMAIL = 'enviodigital@bancochile.cl'


REASON_MAP = {
    'PAYU *UBER TRIP SANTIAGO CHL': 'Transporte',
    'AWTO AWTO CHILE LAS CONDES CL': 'Transporte',
    'AILIME LTDA SANTIAGO CHL': 'Transporte',
    'UBER LAS CONDES CHL': 'Transporte',
    'MERCADOPAGO *CABIFY25 Las Condes CHL': 'Transporte',
    'DLOCAL *AWTO SANTIAGO CL': 'Transporte',
    'LIME*VIAJE ODVY +18885463345 USA': 'Transporte',
    'DLOCAL *UBER RIDES SANTIAGO CHL': 'Transporte',
    'MERCADOPAGO *CABIFYCH Las Condes CHL': 'Transporte',
    'LIME*2 VIAJES ODVY +18885463345 USA': 'Transporte',
    'UBER RIDES UBER RIDES LAS CONDES CHL': 'Transporte',
    'LIME*PAGO ODVY +18885463345 US': 'Transporte',
    'LIME*3 VIAJES ODVY +18885463345 USA': 'Transporte',
    'LIME*VIAJE K4UQ SAN FRANCISCO USA': 'Transporte',

    'PAYU *UBER EATS SANTIAGO CHL': 'Comida - Delivery',

    'HELADERIA LAUSINA SANTIAGO CHL': 'Comida - Restaurantes',
    'NIU SUSHI - ELIECER PA SANTIAGO CHL': 'Comida - Restaurantes',
    'EL TOLDO AZUL SANTIAGO CHL': 'Comida - Restaurantes',
    'LA BURGUESIA SANTIAGO CHL': 'Comida - Restaurantes',
    'NIU SUSHI SANTIAGO CHL': 'Comida - Restaurantes',
    'PAN LEON MUT SPA SANTIAGO CHL': 'Comida - Restaurantes',
    'MP *TANTA SPA SANTIAGO CHL': 'Comida - Restaurantes',
    'MC DONALDS COSTANERA C SANTIAGO CHL': 'Comida - Restaurantes',
    'SOCIEDAD GASTRONOMICA DAISANTIAGO CL': 'Comida - Restaurantes',
    'GASTRONOMIA ZHENG CH SANTIAGO CHL': 'Comida - Restaurantes',
    'TUU*bibimpop SANTIAGO CHL': 'Comida - Restaurantes',
    'CARLS JR SANTIAGO CHL': 'Comida - Restaurantes',
    'RESTAURANT OPORTO SANTIAGO CHL': 'Comida - Restaurantes',
    'TANTA/MUU/CANTINA W SANTIAGO CHL': 'Comida - Restaurantes',
    'RIENDA MUT SPA SANTIAGO CHL': 'Comida - Restaurantes',
    'MERPAGO*SICILY OGGI Q VITACURA CHL': 'Comida - Restaurantes',
    'CRUDO SIN CENSURA MUT LAS CONDES CHL': 'Comida - Restaurantes',

    'OKM SUECIA SANTIAGO CHL': 'Comida - Minimarket',
    'OKM CARMEN SYLVA SANTIAGO CHL': 'Comida - Minimarket',
    'EXPRESS TOBALABA SANTIAGO CHL': 'Comida - Minimarket',
    'EXPRESS PLAZA LYON SANTIAGO CHL': 'Comida - Minimarket',
    'OKM SUECIA SANTIAGO CL': 'Comida - Minimarket',
    'ISIDORA SANTIAGO CHL': 'Comida - Minimarket',
    'SUECIA SANTIAGO CHL': 'Comida - Minimarket',
    'SPID MUT - O871 SANTIAGO CHL': 'Comida - Minimarket',
    'SOCIEDAD GASTRONOMICA SANTIAGO CHL': 'Comida - Minimarket',
    'SERVICIOS Y COMERCIAL LAS CONDES CHL ': 'Comida - Minimarket',
    'MERCADOPAGO *MIMARKET Las Condes CHL': 'Comida - Minimarket',
    'SPID PRESIDENTE RIESCO SANTIAGO CHL': 'Comida - Minimarket',
    'VECINAL SANTIAGO CHL': 'Comida - Minimarket',

    'COURSRA*3I5RRV3WAT9VSM MOUNTAIN VIEWUS': 'Educacion',

    'CRUNCHYROLL LAS CONDES CHL': 'Subscripciones',
    'Smart Fit Chile 551138789999 CL': 'Subscripciones',
    'NETFLIX.COM 866-579-7172 USA': 'Subscripciones',

    'CANDELARIA SANTIAGO CHL': 'Entretenimiento',
    'STEAMGAMES.COM 4259522 BELLEVUE USA': 'Entretenimiento',
    'LOS TRONCOS SANTIAGO CHL': 'Entretenimiento',
    'CINEPLANET WEBPAY SANTIAGO CHL': 'Entretenimiento',
    'CINEPLANET COSTANERA SANTIAGO CHL': 'Entretenimiento',
    'Jagex Payment CAMBRIDGE GBR': 'Entretenimiento',

    'JUMBO COSTANERA CENTER SANTIAGO CHL': 'Supermercado',
    'CHINA HOUSE MARKET SANTIAGO CHL': 'Supermercado',
    'UNIMARC SANTA MARIA SANTIAGO CHL': 'Supermercado',
    'PARIS COSTANERA CENTER SANTIAGO CHL': 'Supermercado',

}
