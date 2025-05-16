# -*- coding: utf-8 -*-

import base64
from datetime import datetime
import json
import logging
from odoo import models, fields, api
from odoo.exceptions import UserError
from odoo.addons.data_fetcher_base.models.odoo_service import OdooService # type: ignore
from ..utils.mappers import map_customer_data, map_company_data, map_product # type: ignore
from ..utils.helpers import ShopifyHelpers

_logger = logging.getLogger(__name__)
helper = ShopifyHelpers()

class ShopifyTransferLog(models.Model):
    _inherit = 'transfer.log'
    _description = 'Shopify Transfer Log'
    
    @api.model
    def fetch_all(self, shopify_service, data):
        print("Fetching all data...")
        log_entry = self.create({
            'name': f'{self.env.user.name}',
            "db_name": data.get('odoo_db'),
            'db_user': data.get('odoo_username'),
            "db_url": f'{data.get("odoo_url")}',
            'db_password': data.get('odoo_api_key'),
            'import_status': 'pending',
            'source': 'Shopify',
        })

        shopify_service.fetch_all(log_entry)

        try:
            cron = self.env.ref('data_fetcher_shopify.ir_cron_shopify_transfer').sudo()
            if not cron.active:
                cron.write({'active': True})
                _logger.info("Activated Shopify transfer cron job after first fetch.")
        except Exception as e:
            _logger.warning(f"Could not activate cron: {str(e)}")

        return "Data fetched successfully"

    @api.model
    def process_all_transfers(self):
        transfers = self.search([
            ('import_status', '=', 'pending'),
            ('source', '=', 'Shopify')
        ])
        _logger.info(f"Found {len(transfers)} pending Shopify transfers.")

        if not transfers:
            _logger.info("No transfers to process.")
            return "No transfers to process."

        all_results = {}

        for transfer in transfers:
            transfer.import_status = 'in_progress'
            results = {}
            failed_categories = set()
            categories_to_process = ['customer', 'product', 'order', 'abandoned_cart']

            try:
                # Extract credentials
                odoo_url = transfer.db_url
                odoo_db = transfer.db_name
                odoo_username = transfer.db_user
                odoo_api_key = transfer.db_password

                if not all([odoo_url, odoo_db, odoo_username, odoo_api_key]):
                    transfer.import_status = 'failed'
                    transfer.error_message = "Missing Odoo credentials."
                    _logger.error(f"Transfer {transfer.name} failed: Missing credentials.")
                    continue

                # Establish Odoo connection
                odoo_service = OdooService(odoo_url, odoo_db, odoo_username, odoo_api_key)
                odoo_service.connect()

                product_variant_map = {}
                _logger.info(f"[{transfer.name}] Prefetching countries...")
                countries = odoo_service.search_read('res.country', [], ['id', 'code'])
                country_map = {
                    country['code'].lower(): country['id']
                    for country in countries if country.get('code')
                }

                for transfer_category in categories_to_process:
                    attachments = self.env['ir.attachment'].search([
                        ('res_model', '=', transfer._name),
                        ('res_id', '=', transfer.id),
                        ('name', 'ilike', f'import_data_{transfer_category}_page_'),
                        ('description', '=', 'pending'),
                    ])

                    if not attachments:
                        _logger.warning(f"[{transfer.name}] No attachments found for category '{transfer_category}'. Skipping.")
                        continue

                    category_failed = False

                    for attachment in attachments:
                        try:
                            decoded = base64.b64decode(attachment.datas)
                            shopify_data = json.loads(decoded.decode('utf-8'))
                        except Exception as decode_err:
                            _logger.error(f"[{transfer.name}] Failed to decode attachment {attachment.name}: {decode_err}", exc_info=True)
                            attachment.write({'description': 'failed'})
                            category_failed = True
                            continue

                        try:
                            if transfer_category == 'customer':
                                _logger.info(f"[{transfer.name}] Importing customers...")
                                customer_result = transfer._import_customers(shopify_data, odoo_service, country_map)

                                results.setdefault('customers', {}).setdefault('company_id_map', {}).update(
                                    customer_result.get('company_id_map', {})
                                )
                                results['customers'].setdefault('customer_map', {}).update(
                                    customer_result.get('customer_map', {})
                                )
                            elif transfer_category == 'product':
                                _logger.info(f"[{transfer.name}] Importing products...")
                                product_result = transfer._import_products(shopify_data, odoo_service)

                                results.setdefault('products', {}).setdefault('product_template_map', {}).update(
                                    product_result.get('product_template_map', {})
                                )
                                results['products'].setdefault('product_variant_map', {}).update(
                                    product_result.get('product_variant_map', {})
                                )

                                product_variant_map.update(product_result.get('product_variant_map', {}))  # still needed for order step

                            elif transfer_category == 'order':
                                _logger.info(f"[{transfer.name}] Importing orders...")
                                order_result = transfer._import_orders('orders', shopify_data, odoo_service, product_variant_map, country_map)

                                results.setdefault('orders', {}).setdefault('order_map', {}).update(
                                    order_result.get('order_map', {})
                                )

                            elif transfer_category == 'abandoned_cart':
                                _logger.info(f"[{transfer.name}] Importing abandoned carts...")
                                cart_result = transfer._import_orders('abandoned_carts', shopify_data, odoo_service, product_variant_map, country_map)

                                results.setdefault('abandoned_carts', {}).setdefault('order_map', {}).update(
                                    cart_result.get('order_map', {})
                                )
                            attachment.write({'description': 'completed'})
                            _logger.info(f"[{transfer.name}] Attachment {attachment.name} processed successfully. Deleting it.")
                            attachment.unlink()

                        except Exception as import_err:
                            attachment.write({'description': 'failed'})
                            _logger.error(f"[{transfer.name}] Error importing {transfer_category}: {import_err}", exc_info=True)
                            category_failed = True
                            continue

                    if category_failed:
                        failed_categories.add(transfer_category)

                # Decide final import_status based on failures
                if len(failed_categories) == len(categories_to_process):
                    transfer.import_status = 'failed'
                    transfer.error_message = f"All categories failed: {', '.join(failed_categories)}"
                    _logger.error(f"[{transfer.name}] Transfer failed completely: {transfer.error_message}")
                elif failed_categories:
                    transfer.import_status = 'completed'
                    transfer.error_message = f"Some categories failed: {', '.join(failed_categories)}"
                    _logger.warning(f"[{transfer.name}] Transfer partially completed with failures in: {', '.join(failed_categories)}")
                else:
                    transfer.import_status = 'completed'
                    transfer.error_message = False
                    transfer.import_date = datetime.now()
                    _logger.info(f"[{transfer.name}] Transfer completed successfully.")
                transfer.db_url = False
                transfer.db_password = False
            except Exception as e:
                transfer.import_status = 'failed'
                transfer.error_message = str(e)
                transfer.db_url = False
                transfer.db_password = False
                _logger.error(f"[{transfer.name}] Transfer failed: {e}", exc_info=True)

            finally:
                all_results[transfer.id] = results
                self.env['ir.attachment'].create({
                    'name': f"shopify_odoo_mapping_{transfer.name}_{transfer.db_user}.json",
                    'type': 'binary',
                    'res_model': transfer._name,
                    'res_id': transfer.id,
                    'datas': base64.b64encode(json.dumps(results, indent=2).encode('utf-8')),
                    'mimetype': 'application/json',
                    'description': 'Mapping of Shopify to Odoo IDs',
                })

                
        _logger.info("All transfers processed. Final results:")
        _logger.info(json.dumps(all_results, indent=2))
        return all_results
    
    def _import_customers(self, shopify_service, odoo_service, country_map):
        """Import customers from Shopify to Odoo using load() with external identifiers"""
        try:
            odoo_service.connect()
            _logger.info('Fetching customers from Shopify...')
            chunk = shopify_service
            _logger.info(f'Fetched {len(chunk)} customers from Shopify')

            created = 0
            skipped = 0
            companies_created = 0
            companies_linked = 0

            company_id_map = {}
            companies_to_create = {}
            customers_data = []
            shopify_odoo_customer_id_map = {}
            shopify_id_list = []

            # First phase: Process companies
            for shopify_customer in chunk:
                company_data = map_company_data(shopify_customer, odoo_service, country_map)
                if company_data:
                    company_name = company_data.pop('name')
                    if not odoo_service.company_exists(company_name):
                        if company_name not in companies_to_create:
                            _logger.info(f'Preparing new company: {company_name}')
                            companies_to_create[company_name] = company_data
                            companies_created += 1

            if companies_to_create:
                for company_name, data in companies_to_create.items():
                    data['name'] = company_name

                first_company = next(iter(companies_to_create.values()))
                company_fields = list(first_company.keys())

                company_values = []
                for data in companies_to_create.values():
                    values = [data.get(field) for field in company_fields]
                    company_values.append(helper.stringify_values(values))

                _logger.info(f'Bulk loading {len(company_values)} companies')
                company_result = odoo_service.load_records('res.partner', company_fields, company_values)
                _logger.info(f'Companies Result: {company_result}')

                if company_result.get('ids'):
                    company_names = list(companies_to_create.keys())
                    for i, company_id in enumerate(company_result['ids']):
                        if i < len(company_names):
                            company_id_map[company_names[i]] = company_id
                    _logger.info(f'Created company ID map: {company_id_map}')

            # Second phase: Process customers with company references 
            customer_fields = None

            for shopify_customer in chunk:
                if odoo_service.customer_exists(shopify_customer['email']):
                    _logger.info(f"Customer with email {shopify_customer['email']} already exists, skipping")
                    skipped += 1
                    continue

                customer_data = map_customer_data(
                    shopify_customer, 
                    odoo_service, 
                    country_map, 
                    company_id_map
                )

                if customer_data.get('parent_id/.id'):
                    companies_linked += 1
                if customer_fields is None:
                    customer_fields = list(customer_data.keys())
                
                values = [customer_data.get(field) for field in customer_fields]
                customers_data.append(helper.stringify_values(values))

                # Track Shopify ID for later mapping
                shopify_id = str(shopify_customer.get('id'))
                shopify_id_list.append(shopify_id)

            if customers_data:
                _logger.info(f'Bulk loading {len(customers_data)} customers')
                customer_result = odoo_service.load_records('res.partner', customer_fields, customers_data)
                created += len(customers_data)
                _logger.info(f'Customers Result: {customer_result}')

                if customer_result.get('ids'):
                    for shopify_id, odoo_id in zip(shopify_id_list, customer_result['ids']):
                        shopify_odoo_customer_id_map[shopify_id] = odoo_id

            return {
                'total': len(chunk),
                'created': created,
                'skipped': skipped,
                'companies_created': companies_created,
                'companies_linked': companies_linked,
                'company_id_map': company_id_map,
                'customer_map': shopify_odoo_customer_id_map
            }

        except Exception as e:
            _logger.error(f'Error importing customers: {str(e)}', exc_info=True)
            raise

    def _import_products(self, shopify_service, odoo_service):
        """Import products from Shopify to Odoo using bulk load method"""
        try:
            odoo_service.connect()
            _logger.info('Fetching products from Shopify...')
            chunk = shopify_service
            _logger.info(f'Fetched {len(chunk)} products from Shopify')
            templates_created = 0
            variants_created = 0
            variants_updated = 0
            
            product_template_map = {}
            product_variant_map = {}
            
            # Pre-fetch all existing product templates to minimize API calls
            _logger.info('Prefetching existing product templates...')
            existing_templates = odoo_service.search_read(
                'product.template',
                [],
                ['id', 'name', 'barcode', 'default_code', 'description']
            )
            
            template_by_shopify_id = {}
            template_by_barcode = {}
            template_by_default_code = {}
            
            for template in existing_templates:
                if template.get('description') and 'shopify_id:' in template['description']:
                    shopify_id = template['description'].split('shopify_id:')[1].split('</p>')[0]
                    template_by_shopify_id[shopify_id] = template
                
                if template.get('barcode'):
                    template_by_barcode[template['barcode']] = template
                if template.get('default_code'):
                    template_by_default_code[template['default_code']] = template
            
                
            templates_to_create = []
            
            # First phase: Process product templates
            for shopify_product in chunk:
                try:
                    product_result = map_product(shopify_product)
                    product_template = product_result['product_template']
                    shopify_id = str(shopify_product.get('id'))
                    
                    existing_template = None                        
                    if shopify_id in template_by_shopify_id:
                        existing_template = template_by_shopify_id[shopify_id]
                        _logger.info(f"Found existing template for {shopify_product.get('title')} by Shopify ID")                        
                    elif product_template.get('barcode') and product_template['barcode'] in template_by_barcode:
                        existing_template = template_by_barcode[product_template['barcode']]
                        _logger.info(f"Found existing template for {shopify_product.get('title')} by barcode")
                    elif product_template.get('default_code') and product_template['default_code'] in template_by_default_code:
                        existing_template = template_by_default_code[product_template['default_code']]
                        _logger.info(f"Found existing template for {shopify_product.get('title')} by default_code")                        

                    if existing_template:
                        _logger.info(f"Skipping existing product {shopify_product.get('title')} (Shopify ID {shopify_id})")
                        continue
                    else:
                        templates_to_create.append(helper.stringify_values([
                            product_template['name'],
                            product_template.get('description_sale', ''),
                            product_template.get('list_price', 0),
                            product_template.get('barcode', shopify_id),
                            product_template.get('default_code', shopify_id),
                            'consu',
                            product_template.get('active', True),
                            f"<p>shopify_id:{shopify_id}</p>",
                            True,
                            True
                        ]))
                        templates_created += 1
                
                except Exception as product_error:
                    _logger.error(f"Error processing product template {shopify_product.get('title')}: {str(product_error)}", exc_info=True)
            if templates_to_create:
                template_fields = [
                    'name', 'description_sale', 'list_price', 'barcode', 'default_code', 
                    'type', 'active', 'description', 'sale_ok', 'purchase_ok'
                ]
                
                _logger.info(f'Bulk loading {len(templates_to_create)} product templates')
                template_result = odoo_service.load_records('product.template', template_fields, templates_to_create)
                _logger.info(f'Template Creation Result: {template_result}')
                
                if template_result.get('ids'):
                    shopify_ids = []
                    for product in chunk:
                        shopify_id = str(product['id'])
                        if (shopify_id not in template_by_shopify_id and 
                            shopify_id not in template_by_barcode and 
                            shopify_id not in template_by_default_code):
                            shopify_ids.append(shopify_id)
                    
                    _logger.info(f'Mapping {len(shopify_ids)} new Shopify IDs to {len(template_result["ids"])} new template IDs')
                    for i, template_id in enumerate(template_result['ids']):
                        if i < len(shopify_ids):
                            product_template_map[shopify_ids[i]] = template_id
            
            for shopify_product in chunk:
                try:
                    shopify_id = str(shopify_product.get('id'))
                    template_id = product_template_map.get(shopify_id)
                    
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
                    
                    product_result = map_product(shopify_product)
                    variants = product_result['product_variants']                        
                    attribute_options = helper._extract_attributes_from_variants(shopify_product)
                    
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
                        shopify_variant = shopify_product['variants'][0]
                        variant = variants[0]

                        existing_variant = None
                        if variant.get('barcode') and variant['barcode'] in variant_by_barcode:
                            existing_variant = variant_by_barcode[variant['barcode']]
                        elif variant.get('default_code') and variant['default_code'] in variant_by_default_code:
                            existing_variant = variant_by_default_code[variant['default_code']]

                        if existing_variant:
                            _logger.info(f"Skipping existing variant for product {shopify_product.get('title')} (Shopify ID {shopify_id})")
                        else:
                            variant_id = helper._handle_single_variant(template_id, variant, product_variant_map, shopify_variant['id'], odoo_service)
                            if variant_id:
                                variants_created += 1
                    else:
                        # Multiple variants
                        variants_result = helper._handle_multiple_variants(
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
                    
                    helper._handle_deleted_variants(template_id, product_variant_map, odoo_service)
                
                except Exception as variant_error:
                    _logger.error(f"Error processing variants for product {shopify_product.get('title')}: {str(variant_error)}", exc_info=True)
        
            return {
                'total': len(chunk),
                'templates_created': templates_created,
                'variants_created': variants_created,
                'product_template_map': product_template_map,
                'product_variant_map': product_variant_map
            }
        
        except Exception as e:
            _logger.error(f'Error importing products: {str(e)}', exc_info=True)
            raise
    
    def _import_orders(self, data_type, shopify_service, odoo_service, product_variant_map, country_map):
        """Import orders or abandoned carts from Shopify to Odoo using bulk load"""
        try:
            odoo_service.connect()
            print(f"product_variant_map: {product_variant_map}")
            is_orders = data_type == 'orders'
            label = 'orders' if is_orders else 'abandoned carts'

            _logger.info(f'Fetching {label} from Shopify...')
            shopify_data = shopify_service
            _logger.info(f'Fetched {len(shopify_data)} {label} from Shopify')

            orders_created = 0
            orders_skipped = 0
            order_lines_created = 0

            orders_to_create = []
            order_lines_to_create = []
            order_ref_to_index = {}
            orders_to_process = []

            # First pass: check which orders need to be created and prepare order data
            for source_data in shopify_data:
                id_field = 'order_number' if is_orders else 'name'
                order_ref = str(source_data[id_field])

                if odoo_service.order_exists(order_ref):
                    _logger.info(f"{'Order' if is_orders else 'Abandoned cart'} #{order_ref} already exists, skipping")
                    orders_skipped += 1
                    continue

                email_field = source_data.get('customer', {}).get('email') if is_orders else source_data.get('email')
                if not email_field:
                    _logger.info(f"{'Order' if is_orders else 'Abandoned cart'} {order_ref} has no customer data, skipping")
                    orders_skipped += 1
                    continue

                existing_customer = odoo_service.customer_exists(email_field)
                if not existing_customer:
                    _logger.info(f"No customer found for email {email_field}, creating new customer for order {order_ref}")
                    customer_data = (map_customer_data(source_data['customer'], odoo_service, country_map) if is_orders
                                    else {'name': source_data['shipping_address']['name'], 'email': email_field})
                    customer_id = odoo_service.create_record('res.partner', customer_data)
                else:
                    customer_id = existing_customer['id']

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

                order = helper._prepare_order_or_cart_for_load(source_data, customer_id, order_ref, is_orders)
                order_index = len(orders_to_create)
                orders_to_create.append(order)
                order_ref_to_index[order_ref] = order_index
                orders_to_process.append({
                    'source_data': source_data,
                    'order_ref': order_ref,
                    'customer_id': customer_id,
                    'valid_line_items': valid_line_items
                })

            if not orders_to_create:
                _logger.info(f"No new {label} to create")
                return {
                    'total': len(shopify_data),
                    'orders_created': 0,
                    'orders_skipped': orders_skipped,
                    'order_lines_created': 0
                }

            order_fields = [
                'partner_id/.id', 'date_order', 'client_order_ref',
                'note', 'state'
            ]

            _logger.info(f"Bulk creating {len(orders_to_create)} {label}")
            order_result = odoo_service.load_records('sale.order', order_fields, orders_to_create)

            if not order_result.get('ids'):
                _logger.error(f"Failed to create {label}: {order_result}")
                return {
                    'total': len(shopify_data),
                    'orders_created': 0,
                    'orders_skipped': orders_skipped,
                    'order_lines_created': 0
                }

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

                odoo_order_id = order_ref_to_odoo_id.get(order_ref)
                if not odoo_order_id:
                    _logger.warning(f"Could not find Odoo ID for {order_ref}, skipping line items and addresses")
                    continue

                for idx, (item, odoo_product_id) in enumerate(valid_line_items):
                    order_line = helper._prepare_order_line_for_load(
                        item,
                        odoo_order_id,
                        odoo_product_id,
                        order_ref,
                        idx
                    )
                    order_lines_to_create.append(order_line)

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
                'order_lines_created': order_lines_created,
                'order_map': order_ref_to_odoo_id
            }

        except Exception as e:
            _logger.error(f'Error importing {data_type}: {str(e)}', exc_info=True)
            raise
