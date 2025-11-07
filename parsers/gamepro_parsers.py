import xml.etree.ElementTree as ET
from ftplib import FTP
import logging
import os
import yaml  # pip install pyyaml якщо немає


def load_supplier_config(config_path='config.yaml'):
    """Завантажує config для постачальника."""
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config['suppliers']['gamepro']  # Тільки GamePro


def download_gamepro_xml(host, user, pass_, file_name):
    """Завантажує XML з FTP GamePro."""
    try:
        ftp = FTP(host)
        ftp.login(user, pass_)
        with open(file_name, "wb") as f:
            ftp.retrbinary(f"RETR {file_name}", f.write)
        ftp.quit()
        logging.info(f"XML завантажено з FTP GamePro: {file_name}")
        return True
    except Exception as e:
        logging.error(f"Помилка FTP GamePro: {e}")
        return False


def parse_gamepro_xml(file_name):
    """Парсинг XML GamePro."""
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

            # Обробка коми
            purchase_price = float(purchase_text.replace(',', '.')) if purchase_text else 0.0
            old_price = float(old_price_text.replace(',', '.')) if old_price_text else None

            # Логіка для supplier_price: RRP якщо є, інакше old_price
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

        logging.info(f"Парсинг XML GamePro: {len(supplier_dict)} товарів")
        return supplier_dict
    except Exception as e:
        logging.error(f"Помилка парсингу XML GamePro: {e}")
        return {}


def parse_gamepro(config_path='config.yaml'):
    """Повний парсинг GamePro з config."""
    config = load_supplier_config(config_path)
    file_name = config['file']
    if download_gamepro_xml(config['host'], config['user'], config['pass'], file_name):
        return parse_gamepro_xml(file_name)
    return {}