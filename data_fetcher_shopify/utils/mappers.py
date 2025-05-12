# mappers.py
from datetime import datetime
import logging

_logger = logging.getLogger(__name__)

def map_company_data(shopify_customer, odoo_service, country_map):
    """Map Shopify customer data to Odoo company format"""
    address = shopify_customer.get('default_address', {}) or {}
    company_name = address.get('company', '').strip() if address.get('company') else False
    
    if not company_name:
        return None
            
    province_code = address.get('province_code', '')
    country_code = address.get('country_code', '').lower() if address.get('country_code') else ''
    province = address.get('province', '')
    
    # Get country ID from map
    country_id = country_map.get(country_code) if country_code else None
    
    # Get state ID from state map
    state_id = None
    if country_id and (province_code or province):
        state_id = odoo_service.get_state_id(country_id, province_code, province)
        
    return {
        'name': company_name,
        'is_company': True,
        'company_type': 'company',
        'customer_rank': 1,
        'street': address.get('address1', ''),
        'street2': address.get('address2', ''),
        'city': address.get('city', ''),
        'zip': address.get('zip', ''),
        'state_id/.id': state_id,
        'country_id/.id': country_id,
        'ref': f"shopify_company_{shopify_customer['id']}",
        'active': True
    }

def map_customer_data(shopify_customer, odoo_service, country_map, company_id_map=None):
    """Map Shopify customer data to Odoo customer format"""
    address = shopify_customer.get('default_address', {}) or {}
    province_code = address.get('province_code', '')
    country_code = address.get('country_code', '').lower() if address.get('country_code') else ''
    province = address.get('province', '')
    
    # Get country ID from map
    country_id = country_map.get(country_code) if country_code else None
    
    # Get state ID from state map
    state_id = None
    if country_id and (province_code or province):
        state_id = odoo_service.get_state_id(country_id, province_code, province)
    
    # Get company info and ID if available
    company_name = address.get('company', '').strip() if address.get('company') else False
    company_id = False
    if company_name:
        # Check if we have the company ID in our map
        if company_name in company_id_map:
            company_id = company_id_map[company_name]

    # Prepare customer data
    full_name = f"{shopify_customer.get('first_name', '')} {shopify_customer.get('last_name', '')}".strip()
    
    return {
        'name': full_name,
        'email': shopify_customer.get('email'),
        'parent_id/.id': company_id,
        'phone': shopify_customer.get('phone'),
        'street': address.get('address1', ''),
        'street2': address.get('address2', ''),
        'city': address.get('city', ''),
        'zip': address.get('zip', ''),
        'state_id/.id': state_id,
        'country_id/.id': country_id,
        'ref': f"shopify_customer_{shopify_customer['id']}",
        'active': True,
        'type': 'contact',
        'customer_rank': 1
    }

def map_product(shopify_product):
    """Maps a Shopify product to Odoo product.template and product.product formats"""
    # Map main product template
    variants = shopify_product.get('variants', [])
    price = float(variants[0].get('price', 0)) if variants else 0
    
    product_template = {
        'name': shopify_product.get('title', ''),
        # 'description': shopify_product.get('body_html', ''),
        'description_sale': shopify_product.get('body_html', ''),
        'list_price': price,
        'barcode': str(shopify_product.get('id')),
        'default_code': str(shopify_product.get('id')),
        'type': 'consu',
        'active': shopify_product.get('status') == 'active',
        'description': f"<p>shopify_id:{shopify_product.get('id')}</p>",
        'sale_ok': True,
        'purchase_ok': True
    }
    
    # Map variants
    product_variants = []
    for variant in shopify_product.get('variants', []):
        # Determine variant name based on options
        if variant.get('title') == 'Default Title' or not variant.get('title'):
            variant_name = shopify_product.get('title', '')
        else:
            # For products with options
            variant_name = f"{shopify_product.get('title', '')} - {variant.get('title', '')}"
        
        product_variants.append({
            'name': variant_name,
            'default_code': variant.get('sku') or str(variant.get('id')),
            'barcode': variant.get('barcode') or str(variant.get('id')),
            'list_price': float(variant.get('price', 0)),
            'standard_price': float(variant.get('price', 0)),
            'weight': float(variant.get('weight', 0))
        })
    
    return {'product_template': product_template, 'product_variants': product_variants}
