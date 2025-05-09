# -*- coding: utf-8 -*-

import base64
from datetime import datetime
import json
import logging
from odoo import models, fields, api
from odoo.exceptions import UserError
from odoo.addons.data_fetcher_base.models.odoo_service import OdooService # type: ignore
from ..utils.mappers import map_product # type: ignore
from ..utils.helpers import ShopifyHelpers

_logger = logging.getLogger(__name__)
product_helper = ShopifyHelpers()

class ShopifyTransferLog(models.Model):
    _inherit = 'transfer.log'
    _description = 'Shopify Transfer Log'
    
    @api.model
    def fetch_all(self, shopify_service):
        print("Fetching all data...")
        shopify_service.fetch_all('customers')
        try:
            cron = self.env.ref('data_fetcher_shopify.ir_cron_shopify_transfer').sudo()
            if not cron.active:
                cron.write({'active': True})
                _logger.info("Activated Shopify transfer cron job after first fetch.")
        except Exception as e:
            _logger.warning(f"Could not activate cron: {str(e)}")
        shopify_service.fetch_all('products')
        shopify_service.fetch_all('orders')
        shopify_service.fetch_all('abandoned_checkouts')
        
        return "Data fetched successfully"
    
    @api.model
    def process_all_transfers(self):
        transfers = self.search([('sync_status', 'in', ['pending', 'failed'])])
        print("Pending Transfers: ", transfers)

        # Fetch Odoo credentials from config
        config = self.env['ir.config_parameter'].sudo()
        odoo_url = config.get_param('odoo.url')
        odoo_db = config.get_param('odoo.db')
        odoo_username = config.get_param('odoo.username')
        odoo_api_key = config.get_param('odoo.password')

        if not all([odoo_url, odoo_db, odoo_username, odoo_api_key]):
            raise UserError("Missing Odoo credentials in system configuration.")

        odoo_service = OdooService(odoo_url, odoo_db, odoo_username, odoo_api_key)
        product_variant_map = {}
        results = {}
        
        for transfer in transfers:
            transfer.sync_status = 'in_progress'
            
            attachment = self.env['ir.attachment'].search([
            ('res_model', '=', transfer._name),
            ('res_id', '=', transfer.id),
            ('name', 'ilike', f'import_data_{transfer.transfer_category}_page_'),
            ], limit=1)

            if not attachment:
                transfer.sync_status = 'failed'
                transfer.error_message = f"No attachment found for transfer: {transfer.name}"
                _logger.error(f"No attachment found for transfer: {transfer.name}")
                continue

            try:
                decoded = base64.b64decode(attachment.datas)
                shopify_data = json.loads(decoded.decode('utf-8'))
                
                if transfer.transfer_category == 'product':
                    _logger.info('Starting product sync...')
                    product_results = transfer._sync_products(shopify_data, odoo_service)
                    results['products'] = product_results
                    print(f"Product Results: {product_results}")
                    product_variant_map.update(product_results.get('product_variant_map', {}))
                elif transfer.transfer_category == 'order':
                    _logger.info('Starting order sync...')
                    results['orders'] = transfer._sync_orders('orders', shopify_data, odoo_service, product_variant_map)
                elif transfer.transfer_category == 'contact':
                    _logger.info('Starting customer sync...')
                    results['customers'] = transfer._sync_customers(shopify_data, odoo_service)
                elif transfer.transfer_category == 'abandoned_cart':
                    _logger.info('Starting abandoned cart sync...')
                    results['abandoned_carts'] = transfer._sync_orders('abandoned_carts', shopify_data, odoo_service, product_variant_map)

                transfer.sync_status = 'completed'
            except Exception as e:
                transfer.sync_status = 'failed'
                transfer.error_message = str(e)
                _logger.error(f"Transfer {transfer.name} failed: {str(e)}", exc_info=True)

        return results

    def _chunk_array(self, array, size=250):
        """Split an array into chunks of specified size"""
        return [array[i:i + size] for i in range(0, len(array), size)]
    
    def _sync_customers(self, shopify_service, odoo_service):
        """Sync customers from Shopify to Odoo using load() with external identifiers"""
        try:
            odoo_service.connect()
            _logger.info('Fetching customers from Shopify...')
            shopify_customers = shopify_service
            _logger.info(f'Fetched {len(shopify_customers)} customers from Shopify')
            print(f"shopify_customers: {shopify_customers}")

            # Prefetch all states for faster lookups
            _logger.info('Prefetching states from Odoo...')
            odoo_service.prefetch_states()

            # Prefetch country IDs for lookup
            _logger.info('Prefetching countries from Odoo...')
            countries = odoo_service.search_read('res.country', [], ['id', 'code'])
            country_map = {country['code'].lower(): country['id'] for country in countries if country.get('code')}

            customer_chunks = self._chunk_array(shopify_customers)
            created = 0
            skipped = 0
            companies_created = 0
            companies_linked = 0
            
            # Dictionary to map company names to their Odoo IDs
            company_id_map = {}

            for index, chunk in enumerate(customer_chunks):
                _logger.info(f'Processing customer chunk {index + 1}/{len(customer_chunks)}')

                companies_to_create = {}
                customers_data = []

                # First phase: Process companies
                for shopify_customer in chunk:
                    address = shopify_customer.get('default_address', {}) or {}
                    company_name = False
                    if shopify_customer.get('default_address', {}) and shopify_customer['default_address'].get('company'):
                        company_name = address.get('company', '').strip()

                    # Track new company if it doesn't already exist
                    if company_name and not odoo_service.company_exists(company_name):
                        if company_name not in companies_to_create:
                            _logger.info(f'Preparing new company: {company_name}')
                            province_code = address.get('province_code', '')
                            country_code = address.get('country_code', '').lower() if address.get('country_code') else ''
                            province = address.get('province', '')
                            
                            # Get country ID from map
                            country_id = country_map.get(country_code) if country_code else None
                            
                            # Get state ID from state map
                            state_id = None
                            if country_id and (province_code or province):
                                state_id = odoo_service.get_state_id(country_id, province_code, province)
                                
                            company_row = self.stringify_values([
                                company_name,
                                True,
                                'company',
                                1,
                                address.get('address1', ''),
                                address.get('address2', ''),
                                address.get('city', ''),
                                address.get('zip', ''),
                                state_id,  # Using state_id directly instead of external ID
                                country_id,  # Using country_id directly instead of external ID
                                f"shopify_company_{shopify_customer['id']}",
                                True
                            ])
                            companies_to_create[company_name] = company_row
                            companies_created += 1

                # Bulk create companies and store their IDs
                company_fields = [
                    'name', 'is_company', 'company_type', 'customer_rank',
                    'street', 'street2', 'city', 'zip',
                    'state_id/.id', 'country_id/.id',  # Changed from state_id/id to state_id/.id
                    'ref', 'active'
                ]
                
                if companies_to_create:
                    _logger.info(f'Bulk loading {len(companies_to_create)} companies')
                    company_result = odoo_service.load_records('res.partner', company_fields, list(companies_to_create.values()))
                    _logger.info(f'Companies Result: {company_result}')
                    
                    # Map company names to their Odoo IDs
                    if company_result.get('ids'):
                        company_names = list(companies_to_create.keys())
                        for i, company_id in enumerate(company_result['ids']):
                            if i < len(company_names):
                                company_id_map[company_names[i]] = company_id
                        _logger.info(f'Created company ID map: {company_id_map}')

                # Second phase: Process customers with company references
                for shopify_customer in chunk:
                    # Skip if customer already exists
                    if odoo_service.customer_exists(shopify_customer['email']):
                        _logger.info(f"Customer with email {shopify_customer['email']} already exists, skipping")
                        skipped += 1
                        continue

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
                    company_name = False
                    company_id = False
                    if shopify_customer.get('default_address', {}) and shopify_customer['default_address'].get('company'):
                        company_name = address.get('company', '').strip()
                        
                        # Check if we have the company ID in our map
                        if company_name in company_id_map:
                            company_id = company_id_map[company_name]
                            companies_linked += 1
                        # If not in our map but exists in Odoo, try to get it
                        elif odoo_service.company_exists(company_name):
                            company_id = odoo_service.get_company_id(company_name)
                            company_id_map[company_name] = company_id
                            companies_linked += 1

                    # Prepare customer row
                    full_name = f"{shopify_customer.get('first_name', '')} {shopify_customer.get('last_name', '')}".strip()
                    customer_row = self.stringify_values([
                        full_name,
                        shopify_customer.get('email'),
                        company_id,  # Now we're passing the correct company ID
                        shopify_customer.get('phone'),
                        address.get('address1', ''),
                        address.get('address2', ''),
                        address.get('city', ''),
                        address.get('zip', ''),
                        state_id,  # Using state_id directly instead of external ID
                        country_id,  # Using country_id directly instead of external ID
                        f"shopify_customer_{shopify_customer['id']}",
                        True,
                        'contact',
                        1
                    ])
                    customers_data.append(customer_row)

                # Bulk create customers
                customer_fields = [
                    'name', 'email', 'parent_id/.id', 'phone', 'street', 'street2', 'city', 'zip',
                    'state_id/.id', 'country_id/.id',  # Changed from state_id/id to state_id/.id
                    'ref', 'active',
                    'type', 'customer_rank'
                ]
                
                if customers_data:
                    _logger.info(f'Bulk loading {len(customers_data)} customers')
                    customer_result = odoo_service.load_records('res.partner', customer_fields, customers_data)
                    created += len(customers_data)
                    _logger.info(f'Customers Result: {customer_result}')

            return {
                'total': len(shopify_customers),
                'created': created,
                'skipped': skipped,
                'companies_created': companies_created,
                'companies_linked': companies_linked
            }

        except Exception as e:
            _logger.error(f'Error syncing customers: {str(e)}', exc_info=True)
            raise

    def _sync_products(self, shopify_service, odoo_service):
        """Sync products from Shopify to Odoo using bulk load method"""
        try:
            odoo_service.connect()
            _logger.info('Fetching products from Shopify...')
            shopify_products = shopify_service
            _logger.info(f'Fetched {len(shopify_products)} products from Shopify')
            
            # Process in chunks
            product_chunks = self._chunk_array(shopify_products)
            templates_created = 0
            templates_updated = 0
            variants_created = 0
            variants_updated = 0
            
            # Map to store Odoo product template IDs keyed by Shopify product ID
            product_template_map = {}
            # Map to store Odoo product variant IDs keyed by Shopify variant ID
            product_variant_map = {}
            
            # Pre-fetch all existing product templates to minimize API calls
            _logger.info('Prefetching existing product templates...')
            existing_templates = odoo_service.search_read(
                'product.template',
                [],
                ['id', 'name', 'barcode', 'default_code', 'description']
            )
            
            # Create lookup maps for faster product matching
            template_by_shopify_id = {}
            template_by_barcode = {}
            template_by_default_code = {}
            
            for template in existing_templates:
                # Extract Shopify ID from description if available
                if template.get('description') and 'shopify_id:' in template['description']:
                    shopify_id = template['description'].split('shopify_id:')[1].split('</p>')[0]
                    template_by_shopify_id[shopify_id] = template
                
                if template.get('barcode'):
                    template_by_barcode[template['barcode']] = template
                if template.get('default_code'):
                    template_by_default_code[template['default_code']] = template
            
            for index, chunk in enumerate(product_chunks):
                _logger.info(f'Processing product chunk {index + 1}/{len(product_chunks)}')
                
                # Lists to hold data for bulk operations
                templates_to_create = []
                templates_to_update = []
                template_update_ids = []
                
                # First phase: Process product templates
                for shopify_product in chunk:
                    try:
                        product_result = map_product(shopify_product)
                        product_template = product_result['product_template']
                        shopify_id = str(shopify_product.get('id'))
                        
                        # Check if product template already exists - more thorough checking
                        existing_template = None
                        
                        # First check by Shopify ID in description
                        if shopify_id in template_by_shopify_id:
                            existing_template = template_by_shopify_id[shopify_id]
                            _logger.info(f"Found existing template for {shopify_product.get('title')} by Shopify ID")
                        
                        # Then check by barcode if set
                        elif product_template.get('barcode') and product_template['barcode'] in template_by_barcode:
                            existing_template = template_by_barcode[product_template['barcode']]
                            _logger.info(f"Found existing template for {shopify_product.get('title')} by barcode")
                        
                        # Then check by default_code if set
                        elif product_template.get('default_code') and product_template['default_code'] in template_by_default_code:
                            existing_template = template_by_default_code[product_template['default_code']]
                            _logger.info(f"Found existing template for {shopify_product.get('title')} by default_code")
                        
                        # As a last resort, search directly in case our cache is out of date
                        elif not existing_template:
                            desc_query = f"<p>shopify_id:{shopify_id}</p>"
                            direct_search = odoo_service.search_read(
                                'product.template',
                                ['|', '|',
                                ('barcode', '=', shopify_id),
                                ('default_code', '=', shopify_id),
                                ('description', 'ilike', desc_query)],
                                ['id', 'name']
                            )
                            if direct_search:
                                existing_template = direct_search[0]
                                _logger.info(f"Found existing template for {shopify_product.get('title')} by direct search")
                        
                        if existing_template:
                            # Update existing template
                            update_data = {
                                'id': existing_template['id'],
                                'name': product_template['name'],
                                'description_sale': product_template.get('description_sale', ''),
                                'list_price': product_template.get('list_price', 0),
                                'active': product_template.get('active', True)
                            }
                            templates_to_update.append(self.stringify_values([
                                update_data['id'],
                                update_data['name'],
                                update_data['description_sale'],
                                update_data['list_price'],
                                update_data['active']
                            ]))
                            template_update_ids.append(existing_template['id'])
                            product_template_map[shopify_id] = existing_template['id']
                            templates_updated += 1
                        else:
                            # Prepare for template creation
                            templates_to_create.append(self.stringify_values([
                                product_template['name'],
                                product_template.get('description_sale', ''),
                                product_template.get('list_price', 0),
                                product_template.get('barcode', shopify_id),
                                product_template.get('default_code', shopify_id),
                                'consu',  # Product type
                                product_template.get('active', True),
                                f"<p>shopify_id:{shopify_id}</p>",
                                True,  # sale_ok
                                True   # purchase_ok
                            ]))
                            templates_created += 1
                    
                    except Exception as product_error:
                        _logger.error(f"Error processing product template {shopify_product.get('title')}: {str(product_error)}", exc_info=True)
                        # Continue to next product
                
                # Bulk create templates
                if templates_to_create:
                    template_fields = [
                        'name', 'description_sale', 'list_price', 'barcode', 'default_code', 
                        'type', 'active', 'description', 'sale_ok', 'purchase_ok'
                    ]
                    
                    _logger.info(f'Bulk loading {len(templates_to_create)} product templates')
                    template_result = odoo_service.load_records('product.template', template_fields, templates_to_create)
                    _logger.info(f'Template Creation Result: {template_result}')
                    
                    # Map new template IDs to Shopify product IDs
                    if template_result.get('ids'):
                        # Create a list of Shopify IDs for newly created templates
                        shopify_ids = []
                        for product in chunk:
                            shopify_id = str(product['id'])
                            # Check if this product should have been created (not existing)
                            if (shopify_id not in template_by_shopify_id and 
                                shopify_id not in template_by_barcode and 
                                shopify_id not in template_by_default_code):
                                shopify_ids.append(shopify_id)
                        
                        _logger.info(f'Mapping {len(shopify_ids)} new Shopify IDs to {len(template_result["ids"])} new template IDs')
                        for i, template_id in enumerate(template_result['ids']):
                            if i < len(shopify_ids):
                                product_template_map[shopify_ids[i]] = template_id
                
                # Bulk update templates
                if templates_to_update:
                    update_fields = [
                        'id', 'name', 'description_sale', 'list_price', 'active'
                    ]
                    
                    _logger.info(f'Bulk updating {len(templates_to_update)} product templates')
                    update_result = odoo_service.load_records('product.template', update_fields, templates_to_update)
                    _logger.info(f'Template Update Result: {update_result}')
                
                # Process variants after templates are created/updated
                # NOTE: For variants, we'll still use the original approach since they require attribute handling
                # which is complex for bulk operations
                for shopify_product in chunk:
                    try:
                        shopify_id = str(shopify_product.get('id'))
                        template_id = product_template_map.get(shopify_id)
                        
                        # If we don't have the template in our map, try to find it one more time
                        if not template_id:
                            _logger.warning(f"Template ID not in map for Shopify product {shopify_id}, trying to find it")
                            desc_query = f"<p>shopify_id:{shopify_id}</p>"
                            direct_search = odoo_service.search_read(
                                'product.template',
                                ['|', '|',
                                ('barcode', '=', shopify_id),
                                ('default_code', '=', shopify_id),
                                ('description', 'ilike', desc_query)],
                                ['id', 'name']
                            )
                            if direct_search:
                                template_id = direct_search[0]['id']
                                product_template_map[shopify_id] = template_id
                                _logger.info(f"Found template ID {template_id} for Shopify product {shopify_id}")
                            else:
                                _logger.error(f"No template ID found for Shopify product {shopify_id}, skipping variants")
                                continue
                        
                        # Extract product variant data
                        product_result = map_product(shopify_product)
                        variants = product_result['product_variants']
                        
                        # Extract attributes
                        attribute_options = product_helper._extract_attributes_from_variants(shopify_product)
                        
                        # Pre-fetch all existing variants for this template
                        existing_variants = odoo_service.search_read(
                            'product.product',
                            [('product_tmpl_id', '=', template_id)],
                            ['id', 'name', 'barcode', 'default_code']
                        )
                        
                        variant_by_barcode = {}
                        variant_by_default_code = {}
                        for variant in existing_variants:
                            if variant.get('barcode'):
                                variant_by_barcode[variant['barcode']] = variant
                            if variant.get('default_code'):
                                variant_by_default_code[variant['default_code']] = variant
                        
                        # Process variants based on how many there are
                        if len(variants) == 1:
                            # Single variant case
                            shopify_variant = shopify_product['variants'][0]
                            variant = variants[0]
                            
                            # Check if variant already exists
                            existing_variant = None
                            if variant.get('barcode') and variant['barcode'] in variant_by_barcode:
                                existing_variant = variant_by_barcode[variant['barcode']]
                            elif variant.get('default_code') and variant['default_code'] in variant_by_default_code:
                                existing_variant = variant_by_default_code[variant['default_code']]
                            
                            if existing_variant:
                                product_variant_map[shopify_variant['id']] = existing_variant['id']
                                
                                # Update existing variant
                                odoo_service.update_record('product.product', existing_variant['id'], {
                                    'weight': variant.get('weight'),
                                    'standard_price': variant.get('standard_price')
                                })
                                variants_updated += 1
                            else:
                                variant_id = product_helper._handle_single_variant(template_id, variant, product_variant_map, shopify_variant['id'], odoo_service)
                                if variant_id:
                                    variants_created += 1
                        else:
                            # Multiple variants - handle attributes
                            # Check if we need to rebuild attributes
                            should_cleanup = product_helper._should_rebuild_attributes(template_id, shopify_product, attribute_options, odoo_service)
                            if should_cleanup:
                                _logger.info(f'Rebuilding attributes for template {template_id}')
                                cleaned = odoo_service.cleanup_product_data(template_id)
                                if not cleaned:
                                    _logger.error(f'Failed to clean up template {template_id}, skipping variants')
                                    continue
                            
                            # Process the variants
                            variants_result = product_helper._handle_multiple_variants(
                                template_id,
                                shopify_product,
                                variants,
                                product_variant_map,
                                attribute_options,
                                odoo_service,
                                variant_by_barcode,
                                variant_by_default_code,
                            )
                            
                            variants_created += variants_result['created']
                            variants_updated += variants_result['updated']
                        
                        # Handle deleted variants
                        shopify_variant_ids = [str(v['id']) for v in shopify_product['variants']]
                        product_helper._handle_deleted_variants(template_id, shopify_variant_ids, product_variant_map, odoo_service)
                    
                    except Exception as variant_error:
                        _logger.error(f"Error processing variants for product {shopify_product.get('title')}: {str(variant_error)}", exc_info=True)
            
            return {
                'total': len(shopify_products),
                'templates_created': templates_created,
                'templates_updated': templates_updated,
                'variants_created': variants_created,
                'variants_updated': variants_updated,
                'product_variant_map': product_variant_map
            }
        
        except Exception as e:
            _logger.error(f'Error syncing products: {str(e)}', exc_info=True)
            raise
    
    def _sync_orders(self, data_type, shopify_service, odoo_service, product_variant_map):
        """Sync orders or abandoned carts from Shopify to Odoo using bulk load"""
        try:
            odoo_service.connect()
            print(f"product_variant_map: {product_variant_map}")
            is_orders = data_type == 'orders'
            label = 'orders' if is_orders else 'abandoned carts'
            
            _logger.info(f'Fetching {label} from Shopify...')
            shopify_data = shopify_service
            _logger.info(f'Fetched {len(shopify_data)} {label} from Shopify')            
            
            # Process in chunks
            data_chunks = self._chunk_array(shopify_data)
            created = 0
            skipped = 0
            
            # Overall stats for reporting
            orders_created = 0
            orders_skipped = 0
            addresses_created = 0
            order_lines_created = 0
            
            for index, chunk in enumerate(data_chunks):
                _logger.info(f'Processing {label} chunk {index + 1}/{len(data_chunks)}')
                
                # Collections for bulk creation
                orders_to_create = []
                order_lines_to_create = []
                delivery_addresses_to_create = []
                billing_addresses_to_create = []
                
                # Maps for tracking relationships
                order_ref_to_index = {}  # Maps order reference to its index in orders_to_create
                orders_to_process = []   # List of orders that passed validation
                
                # First pass: check which orders need to be created and prepare order data
                for source_data in chunk:
                    # Extract identifier based on data type
                    id_field = 'order_number' if is_orders else 'name'
                    order_ref = str(source_data[id_field])
                    
                    # Check if order already exists
                    if odoo_service.order_exists(order_ref):
                        _logger.info(f"{'Order' if is_orders else 'Abandoned cart'} #{order_ref} already exists, skipping")
                        orders_skipped += 1
                        continue
                    
                    # Find customer
                    email_field = source_data.get('customer', {}).get('email') if is_orders else source_data.get('email')
                    if not email_field:
                        _logger.info(f"{'Order' if is_orders else 'Abandoned cart'} {order_ref} has no customer data, skipping")
                        orders_skipped += 1
                        continue
                        
                    existing_customer = odoo_service.customer_exists(email_field)
                    if not existing_customer:
                        _logger.info(f"No customer found for email {email_field}, creating new customer for order {order_ref}")
                        # Create customer (for now, still creating individually, could be improved)
                        customer_data = (self._map_customer(source_data['customer']) if is_orders
                                        else {'name': source_data['shipping_address']['name'], 'email': email_field})
                        
                        customer_id = odoo_service.create_record('res.partner', customer_data)
                    else:
                        customer_id = existing_customer['id']
                    
                    # Validate line items
                    valid_line_items = []
                    for item in source_data.get('line_items', []):
                        variant_id = item.get('variant_id')
                        odoo_product_id = product_variant_map.get(variant_id, False)
                        if odoo_product_id:
                            valid_line_items.append((item, odoo_product_id))
                    
                    if not valid_line_items:
                        _logger.info(f"{'Order' if is_orders else 'Abandoned cart'} {order_ref} has no valid line items, skipping")
                        orders_skipped += 1
                        continue
                    
                    # Prepare order data for bulk creation
                    if is_orders:
                        order = self._prepare_order_for_load(source_data, customer_id, order_ref)
                    else:
                        order = self._prepare_abandoned_cart_for_load(source_data, customer_id, order_ref)
                    
                    # Add to orders collection
                    order_index = len(orders_to_create)
                    orders_to_create.append(order)
                    order_ref_to_index[order_ref] = order_index
                    
                    # Store for processing line items and addresses in next phase
                    orders_to_process.append({
                        'source_data': source_data,
                        'order_ref': order_ref,
                        'customer_id': customer_id,
                        'valid_line_items': valid_line_items
                    })
                
                # Bulk create orders if any
                if not orders_to_create:
                    _logger.info(f"No new {label} to create in this chunk")
                    continue
                    
                # Define order fields for load
                order_fields = [
                    'partner_id/.id', 'date_order', 'client_order_ref', 
                    'note', 'state'
                ]
                
                # Bulk create orders
                _logger.info(f"Bulk creating {len(orders_to_create)} {label}")
                order_result = odoo_service.load_records('sale.order', order_fields, orders_to_create)
                
                if not order_result.get('ids'):
                    _logger.error(f"Failed to create {label}: {order_result}")
                    continue
                    
                # Create mapping from order reference to Odoo ID
                order_ref_to_odoo_id = {}
                for i, order_id in enumerate(order_result['ids']):
                    if i < len(orders_to_process):
                        order_ref = orders_to_process[i]['order_ref']
                        order_ref_to_odoo_id[order_ref] = order_id
                
                orders_created += len(order_result['ids'])
                _logger.info(f"Created {len(order_result['ids'])} {label} in Odoo")
                
                # Second pass: Process addresses and order lines
                for order_data in orders_to_process:
                    source_data = order_data['source_data']
                    order_ref = order_data['order_ref']
                    customer_id = order_data['customer_id']
                    valid_line_items = order_data['valid_line_items']
                    
                    # Get Odoo order ID
                    odoo_order_id = order_ref_to_odoo_id.get(order_ref)
                    if not odoo_order_id:
                        _logger.warning(f"Could not find Odoo ID for {order_ref}, skipping line items and addresses")
                        continue
                    
                    # Prepare order lines
                    for idx, (item, odoo_product_id) in enumerate(valid_line_items):
                        order_line = self._prepare_order_line_for_load(
                            item, 
                            odoo_order_id, 
                            odoo_product_id,
                            order_ref,
                            idx
                        )
                        order_lines_to_create.append(order_line)
                
                # Bulk create addresses
                address_fields = [
                    'name', 'parent_id/.id', 'type', 'street', 'street2',
                    'city', 'zip', 'state_id/.id', 'country_id/.id', 
                    'phone', 'email'
                ]
                
                if delivery_addresses_to_create:
                    _logger.info(f"Bulk creating {len(delivery_addresses_to_create)} delivery addresses")
                    address_result = odoo_service.load_records('res.partner', address_fields, delivery_addresses_to_create)
                    if address_result.get('ids'):
                        addresses_created += len(address_result['ids'])
                
                if billing_addresses_to_create:
                    _logger.info(f"Bulk creating {len(billing_addresses_to_create)} billing addresses")
                    address_result = odoo_service.load_records('res.partner', address_fields, billing_addresses_to_create)
                    if address_result.get('ids'):
                        addresses_created += len(address_result['ids'])
                
                # Bulk create order lines
                if order_lines_to_create:
                    order_line_fields = [
                        'order_id/.id', 'product_id/.id', 'name', 
                        'product_uom_qty', 'price_unit'
                    ]
                    
                    _logger.info(f"Bulk creating {len(order_lines_to_create)} order lines")
                    line_result = odoo_service.load_records('sale.order.line', order_line_fields, order_lines_to_create)
                    if line_result.get('ids'):
                        order_lines_created += len(line_result['ids'])
            
            return {
                'total': len(shopify_data),
                'orders_created': orders_created,
                'orders_skipped': orders_skipped,
                'addresses_created': addresses_created,
                'order_lines_created': order_lines_created
            }
        
        except Exception as e:
            _logger.error(f'Error syncing {data_type}: {str(e)}', exc_info=True)
            raise

    def _prepare_order_for_load(self, shopify_order, customer_id, order_ref):
        """Prepare order data for bulk loading"""
        return self.stringify_values([
            customer_id,  # partner_id/.id
            self._convert_shopify_date_to_odoo_format(shopify_order.get('created_at')),  # date_order
            order_ref,  # client_order_ref
            shopify_order.get('note', ''),  # note
            self._find_order_state(shopify_order),  # state
            # f"shopify_order_{order_ref}"  # ref (external ID)
        ])

    def _prepare_abandoned_cart_for_load(self, shopify_cart, customer_id, cart_ref):
        """Prepare abandoned cart data for bulk loading"""
        return self.stringify_values([
            customer_id,  # partner_id/.id
            self._convert_shopify_date_to_odoo_format(shopify_cart.get('created_at')),  # date_order
            cart_ref,  # client_order_ref
            shopify_cart.get('note', ''),  # note
            'draft',  # state
            # f"shopify_abandoned_cart_{cart_ref}"  # ref (external ID)
        ])

    def _prepare_order_line_for_load(self, item, odoo_order_id, odoo_product_id, order_ref, idx):
        """Prepare order line data for bulk loading"""
        # Handle variant title for name
        name = item.get('name', '')
        if item.get('variant_title'):
            name = f"{name} - {item.get('variant_title')}"
        
        return self.stringify_values([
            odoo_order_id,  # order_id/.id
            odoo_product_id,  # product_id/.id
            name,  # name
            item.get('quantity', 0),  # product_uom_qty
            float(item.get('price', 0)),  # price_unit
            # f"shopify_order_line_{order_ref}_{idx}"  # ref (external ID)
        ])

    def _map_customer(self, customer_data):
        """Map Shopify customer data to Odoo format"""
        return {
            'name': f"{customer_data.get('first_name', '')} {customer_data.get('last_name', '')}".strip(),
            'email': customer_data.get('email', ''),
            'phone': customer_data.get('phone', ''),
        }

    def _convert_shopify_date_to_odoo_format(self, shopify_date):
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

    def _find_order_state(self, shopify_order):
        """Determine appropriate order state based on Shopify order data"""
        fulfillment_status = shopify_order.get('fulfillment_status')
        financial_status = shopify_order.get('financial_status')
        
        if fulfillment_status == 'fulfilled':
            return 'sale'
        elif fulfillment_status is None:
            if financial_status == 'paid':
                return 'sale'
            elif financial_status == 'partially_paid':
                return 'sent'
            elif financial_status == 'refunded':
                return 'cancel'
        elif fulfillment_status == 'restocked':
            return 'cancel'
        else:
            return 'draft'
        
    def stringify_values(self, row):
        return [str(val) if isinstance(val, (int, float, bool)) else (val or '') for val in row]

    