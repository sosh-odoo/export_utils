# mappers.py
from datetime import datetime
import logging

_logger = logging.getLogger(__name__)

def map_address(source, parent_contact_id, address_type, country_id=False, state_id=False):
    """Maps Shopify address to Odoo res.partner address format"""
    is_order = 'order_number' in source
    id_field = 'order_number' if is_order else 'name'
    ref_prefix = 'order' if is_order else 'abandoned_cart'
    address_key = 'shipping_address' if address_type == 'delivery' else 'billing_address'
    
    if address_key not in source or not source[address_key]:
        return False
    
    address = source[address_key]
    return {
        'type': 'delivery' if address_type == 'delivery' else 'invoice',
        'parent_id': parent_contact_id,
        'name': f"{address.get('first_name', '')} {address.get('last_name', '')}".strip(),
        'phone': address.get('phone') or source.get('phone'),
        'email': source.get('email'),
        'street': address.get('address1', ''),
        'street2': address.get('address2', ''),
        'city': address.get('city', ''),
        'zip': address.get('zip', ''),
        'state_id': state_id,
        'country_id': country_id,
        'comment': f"{address_type.capitalize()} address from Shopify {ref_prefix} #{source.get(id_field)}",
        'ref': f"shopify_{address_type}_{source.get('id')}",
        'active': True,
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

def convert_shopify_date_to_odoo_format(shopify_date):
    """Converts Shopify ISO 8601 date to Odoo datetime format"""
    try:
        if not shopify_date:
            return False
        
        # Parse the ISO 8601 date
        date_obj = datetime.fromisoformat(shopify_date.replace('Z', '+00:00'))
        
        # Format to YYYY-MM-DD HH:MM:SS
        formatted_date = date_obj.strftime('%Y-%m-%d %H:%M:%S')
        
        return formatted_date
    except Exception as e:
        _logger.error(f"Error converting date {shopify_date}: {str(e)}")
        return False

def find_order_state(shopify_order):
    """Determines the Odoo order state based on Shopify order status"""
    fulfillment_status = shopify_order.get('fulfillment_status')
    financial_status = shopify_order.get('financial_status')
    
    if fulfillment_status == 'fulfilled':
        return 'sale'
    elif fulfillment_status is None:
        if financial_status == 'paid':
            return 'sale'
        elif financial_status == 'refunded':
            return 'cancel'
    elif fulfillment_status == 'restocked':
        return 'cancel'
    else:
        return 'draft'