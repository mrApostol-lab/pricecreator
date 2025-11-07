import aiohttp
import yaml
import logging
from .gamepro_parsers import download_gamepro_xml, parse_gamepro_xml  # Для FTP


async def load_supplier_config(config_path='config.yaml'):
    """Завантажує config для всіх постачальників."""
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config['suppliers']


async def download_http_file(url, file_name):
    """Скачує файл з URL (для method='http')."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    with open(file_name, 'wb') as f:
                        f.write(await resp.read())
                    logging.info(f"Файл завантажено з {url}: {file_name}")
                    return True
    except Exception as e:
        logging.error(f"Помилка HTTP скачування {url}: {e}")
    return False


def parse_xml_file(file_name):
    """Загальний парсер XML (як у GamePro, для будь-якого)."""
    # Тут копіюємо логіку з parse_gamepro_xml — для уніфікації
    import xml.etree.ElementTree as ET
    try:
        tree = ET.parse(file_name)
        root = tree.getroot()
        supplier_offers = root.findall(".//offer")

        supplier_dict = {}
        for sup_offer in supplier_offers:
            supplier_id = sup_offer.get("id")
            if not supplier_id:
                continue

            available_attr = sup_offer.get("available", "false")
            available = "true" if available_attr.lower() == "true" else "false"
            stock_qty = "100" if available == "true" else "0"

            purchase_text = sup_offer.findtext("price") or "0"
            old_price_text = sup_offer.findtext("price_rrp") or None
            rrp_price_text = sup_offer.findtext("price_promo_rrp") or None

            purchase_price = float(purchase_text.replace(',', '.')) if purchase_text else 0.0
            old_price = float(old_price_text.replace(',', '.')) if old_price_text else None

            is_rrp_fallback = False
            if rrp_price_text and float(rrp_price_text.replace(',', '.')) > 0:
                supplier_price = float(rrp_price_text.replace(',', '.'))
            elif old_price_text:
                supplier_price = old_price
                is_rrp_fallback = True
            else:
                supplier_price = 0.0
                is_rrp_fallback = True

            supplier_dict[supplier_id] = {
                'purchase_price': purchase_price,
                'supplier_price': supplier_price,
                'old_price': old_price,
                'available': available,
                'stock_quantity': int(stock_qty),
                'is_rrp_fallback': is_rrp_fallback
            }

        logging.info(f"Парсинг XML: {len(supplier_dict)} товарів")
        return supplier_dict
    except Exception as e:
        logging.error(f"Помилка парсингу XML: {e}")
        return {}


async def parse_supplier(supplier_name, config):
    """Загальний парсер для постачальника з config."""
    method = config['method']
    file_name = config.get('file', f"{supplier_name}_prices.xml")

    if method == 'ftp':
        # GamePro
        host = config['host']
        user = config['user']
        pass_ = config['pass']
        if download_gamepro_xml(host, user, pass_, file_name):
            return parse_xml_file(file_name)
    elif method == 'http':
        # З сайту
        url = config['url']
        if await download_http_file(url, file_name):
            return parse_xml_file(file_name)
    elif method == 'local':
        # Локальний файл
        if os.path.exists(file_name):
            return parse_xml_file(file_name)
    else:
        logging.error(f"Невідомий method '{method}' для {supplier_name}")

    return {}

# Для зручності — async, бо HTTP aiohttp