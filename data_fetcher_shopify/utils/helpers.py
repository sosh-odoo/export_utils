# -*- coding: utf-8 -*-
from datetime import datetime
import logging
import time

_logger = logging.getLogger(__name__)

class ShopifyHelpers:

    def _handle_deleted_variants(self, template_id, product_variant_map, odoo_service):
        """Archive variants that exist in Odoo but not in Shopify"""
        try:
            # Get all variants for this template in Odoo
            odoo_variants = odoo_service.search_read(
                'product.product',
                [('product_tmpl_id', '=', template_id), ('active', '=', True)],
                ['id', 'default_code', 'barcode']
            )
            
            # Get the Odoo variant IDs that were processed from Shopify
            processed_odoo_variant_ids = list(product_variant_map.values())
            
            # Find variants in Odoo that weren't processed (meaning they were deleted in Shopify)
            variants_to_archive = [
                variant['id'] for variant in odoo_variants
                if variant['id'] not in processed_odoo_variant_ids
            ]
            
            if variants_to_archive:
                _logger.info(f'Found {len(variants_to_archive)} variants to archive for template {template_id}')
                
                # Archive the variants (don't delete to preserve history)
                for variant_id in variants_to_archive:
                    odoo_service.update_record('product.product', variant_id, ({'active': False}))
                    _logger.info(f'Archived variant {variant_id}')
            
            return len(variants_to_archive)
        except Exception as e:
            _logger.error(f'Error handling deleted variants: {str(e)}', exc_info=True)
            return 0

    def _handle_single_variant(self, template_id, variant, product_variant_map, shopify_variant_id, odoo_service):
        """Handle creation or update of a single product variant"""
        try:
            # Check if variant already exists by barcode OR default_code
            domain = ['|', 
                    ('barcode', '=', variant.get('barcode')), 
                    ('default_code', '=', variant.get('default_code')),
                    ('product_tmpl_id', '=', template_id)]
            
            existing_variant = odoo_service.search_read(
                'product.product',
                domain, ['id', 'name', 'barcode', 'default_code']
            )
            
            if existing_variant:
                _logger.info(f"Product variant {variant.get('name')} already exists, using existing variant")
                variant_id = existing_variant[0]['id']
                product_variant_map[shopify_variant_id] = variant_id
                
                # Update the existing variant but preserve identifiers
                # odoo_service.update_record('product.product', variant_id, {
                #     'weight': variant.get('weight'),
                #     'standard_price': variant.get('standard_price')
                # })
                
                return variant_id
            
            # First, check if this is the only variant for this template
            variant_c = odoo_service.search_read(
                'product.product',
                [('product_tmpl_id', '=', template_id)],
                ['id']
            )
            
            variant_count = len(variant_c)
            
            if variant_count == 1:
                # Find the existing variant
                existing_variants = odoo_service.search_read(
                    'product.product',
                    [('product_tmpl_id', '=', template_id)],
                    ['id']
                )
                
                if existing_variants:
                    # Update the existing variant instead of creating a new one
                    variant_id = existing_variants[0]['id']
                    # odoo_service.update_record('product.product', variant_id, {
                    #     'default_code': variant.get('default_code'),
                    #     'barcode': variant.get('barcode'),
                    #     'weight': variant.get('weight'),
                    #     'standard_price': variant.get('standard_price')
                    # })
                    
                    product_variant_map[shopify_variant_id] = variant_id
                    _logger.info(f"Updated default variant for template {template_id}")
                    return variant_id
            
            # If we need to create a new variant
            _logger.info(f"Creating new variant for template {template_id}")
            variant_data = {
                'product_tmpl_id': template_id,
                'default_code': variant.get('default_code'),
                'barcode': variant.get('barcode'),
                'weight': variant.get('weight'),
                'standard_price': variant.get('standard_price'),
                'product_template_attribute_value_ids': [(6, 0, [])]  # Empty array ensures unique combination
            }
            
            variant_id = odoo_service.create_record('product.product', variant_data)
            product_variant_map[shopify_variant_id] = variant_id
            
            return variant_id
        except Exception as e:
            _logger.error(f'Error handling single variant for template {template_id}: {str(e)}', exc_info=True)
            return None

    def _handle_multiple_variants(self, template_id, shopify_product, variants, product_variant_map, attribute_options, odoo_service, variant_by_barcode={}, variant_by_default_code={} ):
        """Handle creation of multiple product variants with attributes"""
        try:
            result = {'created': 0, 'updated': 0}
            # template = request.env['product.template'].browse(template_id)
            # template_name = shopify_product['title']
            
            # Check if this product already has attribute lines
            existing_attr_lines = odoo_service.search_read(
                'product.template.attribute.line',
                [('product_tmpl_id', '=', template_id)],
                ['id', 'attribute_id', 'value_ids']
            )
            
            # Store attribute line IDs by attribute name for lookup
            attribute_line_map = {}
            
            # If we have existing attribute lines, use them instead of creating new ones
            if existing_attr_lines and len(existing_attr_lines) > 0:
                for line in existing_attr_lines:
                    attr = odoo_service.get_attribute_name(line['attribute_id'][0])
                    attr_name = attr
                    if attr_name:
                        attribute_line_map[attr_name] = {
                            'lineId': line['id'],
                            'attributeId': line['attribute_id'][0],
                            'valueIds': line['value_ids']
                        }
            
            # Set up or reuse attributes in Odoo
            attribute_data = {}
            
            for attribute_name, attribute_values in attribute_options.items():
                # Skip processing if this attribute already exists with same values
                if attribute_name in attribute_line_map:
                    existing_line = attribute_line_map[attribute_name]
                    existing_values = odoo_service.search_read(
                        'product.attribute.value',
                        [('id', 'in', existing_line['valueIds'])],
                        ['id', 'name']
                    )
                    
                    existing_value_names = [v['name'] for v in existing_values]
                    all_values_exist = all(
                        any(name.lower() == value.lower() for name in existing_value_names)
                        for value in attribute_values
                    )
                    
                    if all_values_exist:
                        # Use existing attribute line and values
                        value_map = {val['name'].lower(): val['id'] for val in existing_values}
                        attribute_data[attribute_name] = {
                            'lineId': existing_line['lineId'],
                            'attributeId': existing_line['attributeId'],
                            'valueMap': value_map
                        }
                        continue
                
                # Find or create attribute
                attribute_id = odoo_service.find_or_create_attribute(attribute_name)
                
                # Create or update attribute values
                value_map = {}
                for value in attribute_values:
                    value_id = odoo_service.find_or_create_attribute_value(attribute_id, value)
                    value_map[value.lower()] = value_id
                
                # Create attribute line for the template if it doesn't exist
                if attribute_name in attribute_line_map:
                    line_id = attribute_line_map[attribute_name]['lineId']
                    # Update existing line with any new values
                    # value_ids = list(value_map.values())
                    # odoo_service.update_record('product.template.attribute.line', line_id, ({
                    #     'value_ids': [(6, 0, value_ids)]
                    # }))
                else:
                    line_id = odoo_service.create_record('product.template.attribute.line', ({
                        'product_tmpl_id': template_id,
                        'attribute_id': attribute_id,
                        'value_ids': [(6, 0, list(value_map.values()))]
                    }))
                    _logger.info(f"Created attribute line for {attribute_name} with values: {', '.join(attribute_values)}")
                
                attribute_data[attribute_name] = {
                    'lineId': line_id,
                    'attributeId': attribute_id,
                    'valueMap': value_map
                }
            
            # Important: Wait for attribute creation to propagate in Odoo before creating variants
            time.sleep(0.5)
            
            # Fetch the latest template attribute values for this template
            ptav_map = self._build_ptav_map(template_id, odoo_service)
            
            # Process each variant to link with its attribute values
            for index, variant in enumerate(variants):
                try:
                    shopify_variant = shopify_product['variants'][index]
                    
                    # Check if variant already exists by barcode OR default_code
                    existing_variant = None
                    if variant.get('barcode') and variant['barcode'] in variant_by_barcode:
                        existing_variant = variant_by_barcode[variant['barcode']]
                    elif variant.get('default_code') and variant['default_code'] in variant_by_default_code:
                        existing_variant = variant_by_default_code[variant['default_code']]
                    else:
                        # If not in our pre-fetched maps, check directly
                        domain = [
                            '|',
                            ('barcode', '=', variant.get('barcode')),
                            ('default_code', '=', variant.get('default_code')),
                            ('product_tmpl_id', '=', template_id)
                        ]
                        existing_variant_records = odoo_service.search_read(
                            'product.product',
                            domain,
                            ['id', 'name', 'barcode', 'default_code'],
                        )
                        
                        if existing_variant_records:
                            existing_variant = existing_variant_records[0]
                    
                    if existing_variant:
                        _logger.info(f"Product variant {variant['name']} already exists, updating")
                        product_variant_map[shopify_variant['id']] = existing_variant['id']
                        
                        # Update existing variant
                        # odoo_service.update_record('product.product', existing_variant['id'], ({
                        #     'weight': variant.get('weight', 0.0),
                        #     'standard_price': variant.get('standard_price', 0.0)
                        # }))
                        
                        result['updated'] += 1
                        continue
                    
                    # Find or create the variant with attribute values
                    variant_id = self._create_variant_with_attributes(
                        template_id,
                        variant,
                        attribute_options,
                        shopify_variant,
                        product_variant_map,
                        ptav_map,
                        odoo_service
                    )
                    
                    if variant_id:
                        # Add to lookup maps for future reference
                        if variant.get('barcode'):
                            variant_by_barcode[variant['barcode']] = {'id': variant_id}
                        if variant.get('default_code'):
                            variant_by_default_code[variant['default_code']] = {'id': variant_id}
                        result['created'] += 1
                except Exception as variant_error:
                    _logger.error(f"Error processing variant {variant.get('name')}: {str(variant_error)}", exc_info=True)
            
            return result
        except Exception as e:
            _logger.error(f'Error handling multiple variants for template {template_id}: {str(e)}', exc_info=True)
            return {'created': 0, 'updated': 0}

    def _build_ptav_map(self, template_id, odoo_service):
        """Build a map of product template attribute values for quick lookup"""
        try:
            # Get all product template attribute values for this template
            ptavs = odoo_service.search_read(
                'product.template.attribute.value',
                [('product_tmpl_id', '=', template_id)],
                ['id', 'product_attribute_value_id', 'attribute_id']
            )
            
            # Create a map for quick lookup: {attributeId: {valueId: ptavId}}
            ptav_map = {}
            
            for ptav in ptavs:
                attribute_id = ptav['attribute_id'][0]
                value_id = ptav['product_attribute_value_id'][0]
                
                if attribute_id not in ptav_map:
                    ptav_map[attribute_id] = {}
                
                ptav_map[attribute_id][value_id] = ptav['id']
            
            return ptav_map
        except Exception as error:
            _logger.error(f"Error building PTAV map: {str(error)}", exc_info=True)
            return {}

    def _create_variant_with_attributes(self, template_id, variant, attribute_options, shopify_variant, product_variant_map, ptav_map, odoo_service):
        """Create a product variant with the specified attributes"""
        try:
            # Extract variant attribute values from variant name
            variant_name = variant.get('name', '')
            product_name = variant_name.split(' - ')[0]
            attribute_part = variant_name.replace(f"{product_name} - ", '')
            
            # Get attribute lines for the template
            attr_lines = odoo_service.search_read(
                'product.template.attribute.line',
                [('product_tmpl_id', '=', template_id)],
                ['id', 'attribute_id', 'value_ids']
            )
            
            # Find matching PTAVs for this variant
            ptav_ids = []
            
            for attr_line in attr_lines:
                attribute_id = attr_line['attribute_id'][0]
                attribute_name = odoo_service.get_attribute_name(attribute_id)
                
                # Skip if we don't have any options for this attribute
                if attribute_name not in attribute_options:
                    continue
                
                # Try to find a matching value
                match_found = False
                
                # Sort attribute values by length (descending) to match longer values first
                sorted_values = sorted(
                    attribute_options[attribute_name],
                    key=lambda x: len(x),
                    reverse=True
                )
                
                for attr_value in sorted_values:
                    # Check if the value is in the variant name
                    if attr_value in attribute_part:
                        # Find the attribute value ID
                        attr_value_id = odoo_service.find_attribute_value_id(
                            attribute_id,
                            attr_value
                        )
                        
                        if attr_value_id and attribute_id in ptav_map and attr_value_id in ptav_map[attribute_id]:
                            ptav_ids.append(ptav_map[attribute_id][attr_value_id])
                            match_found = True
                            break
                
                # If no match found for this attribute, use the first PTAV for this attribute line
                if not match_found:
                    ptavs = odoo_service.search_read(
                        'product.template.attribute.value',
                        [
                            ('product_tmpl_id', '=', template_id),
                            ('attribute_id', '=', attribute_id)
                        ],
                        ['id'],
                    )
                    
                    if ptavs:
                        ptav_ids.append(ptavs[0]['id'])
            
            # Check if a variant with these exact PTAVs already exists
            if ptav_ids:
                exact_domain = [
                    ('product_tmpl_id', '=', template_id),
                    ('product_template_attribute_value_ids', 'in', ptav_ids)
                ]
                
                exact_matches = odoo_service.search_read(
                    'product.product',
                    exact_domain,
                    ['id', 'product_template_attribute_value_ids']
                )
                
                # Find a variant with the exact combination of PTAVs
                exact_match = None
                for v in exact_matches:
                    v_ptav_ids = v['product_template_attribute_value_ids']
                    if (len(v_ptav_ids) == len(ptav_ids) and 
                        all(ptav_id in v_ptav_ids for ptav_id in ptav_ids)):
                        exact_match = v
                        break
                
                if exact_match:
                    _logger.info("Variant with exact attribute combinations already exists, using existing variant")
                    product_variant_map[shopify_variant['id']] = exact_match['id']
                    
                    # Update existing variant but keep identifiers
                    # odoo_service.update_record('product.product', exact_match['id'], ({
                    #     'default_code': variant.get('default_code'),
                    #     'barcode': variant.get('barcode'),
                    #     'weight': variant.get('weight', 0.0),
                    #     'standard_price': variant.get('standard_price', 0.0)
                    # }))
                    
                    return exact_match['id']
            
            # First check if a variant with this barcode already exists ANYWHERE in Odoo
            # This prevents barcode conflicts across the entire database
            if variant.get('barcode'):
                existing_barcode_variant = odoo_service.search_read(
                    'product.product',
                    [('barcode', '=', variant['barcode'])],
                    ['id', 'name', 'barcode'],
                )
                
                if existing_barcode_variant:
                    _logger.warning(f"Warning: Variant with barcode {variant['barcode']} already exists elsewhere in Odoo")
                    # Generate a new unique barcode or set to null to avoid conflict
                    variant['barcode'] = None  # Or implement a barcode generation strategy
            
            # Create the variant with attribute values
            _logger.info(f"Creating variant \"{variant.get('name', '')}\" with attribute values: {ptav_ids}")
            variant_data = {
                'product_tmpl_id': template_id,
                'default_code': variant.get('default_code'),
                'barcode': variant.get('barcode'),
                'weight': variant.get('weight', 0.0),
                'standard_price': variant.get('standard_price', 0.0)
            }
            
            # Only set product_template_attribute_value_ids if we have values
            if ptav_ids:
                variant_data['product_template_attribute_value_ids'] = [(6, 0, ptav_ids)]
            
            try:
                variant_id = odoo_service.create_record('product.product', variant_data)
                product_variant_map[shopify_variant['id']] = variant_id
                return variant_id
            except Exception as error:
                # If creation fails due to barcode conflict, try again without barcode
                if "Barcode(s) already assigned" in str(error):
                    _logger.info('Barcode conflict detected, retrying without barcode')
                    variant_data['barcode'] = None
                    variant_record = odoo_service.create_record('product.product', variant_data)
                    variant_id = variant_id
                    product_variant_map[shopify_variant['id']] = variant_id
                    return variant_id
                else:
                    raise
        except Exception as error:
            _logger.error(f"Error creating variant with attributes: {str(error)}", exc_info=True)
            return None

    def _extract_attributes_from_variants(self, shopify_product):
        """Extract attribute options from Shopify product"""
        attribute_options = {}
        
        # Check if Shopify provides structured options
        if shopify_product.get('options') and isinstance(shopify_product['options'], list):
            options_with_values = [opt for opt in shopify_product['options'] 
                                if opt.get('values') and len(opt['values']) > 0]
            
            if options_with_values:
                # Use structured options from Shopify
                for option in options_with_values:
                    if option.get('values') and len(option['values']) > 0:
                        attribute_options[option['name']] = list(set(option['values']))
        
        _logger.info(f'Extracted attribute options: {attribute_options}')
        return attribute_options
    
    def _prepare_order_or_cart_for_load(self, data, customer_id, ref, is_order=True):
        """Prepare order or abandoned cart data for bulk loading"""
        return self.stringify_values([
            customer_id,
            self._convert_shopify_date_to_odoo_format(data.get('created_at')),
            ref,
            data.get('note', ''),
            self._find_order_state(data) if is_order else 'draft',
        ])

    def _prepare_order_line_for_load(self, item, odoo_order_id, odoo_product_id, order_ref, idx):
        """Prepare order line data for bulk loading"""
        # Handle variant title for name
        name = item.get('name', '')
        if item.get('variant_title'):
            name = f"{name} - {item.get('variant_title')}"
        
        return self.stringify_values([
            odoo_order_id,
            odoo_product_id,
            name,
            item.get('quantity', 0),
            float(item.get('price', 0)),
        ])

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
