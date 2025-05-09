# -*- coding: utf-8 -*-

import logging

from odoo import http # type: ignore
from odoo.http import request # type: ignore
from odoo.exceptions import UserError
from odoo.addons.data_fetcher_base.controllers.main import SyncController as BaseSyncController # type: ignore
from ..utils.shopify_service import ShopifyService
_logger = logging.getLogger(__name__)

class ShopifySyncController(BaseSyncController):
    
    def _handle_shopify(self, data):
        """Handle Shopify data transfer"""
        try:
            # Extract parameters
            shopify_url = data.get('shopify_url')
            shopify_token = data.get('shopify_token')
            
            if not shopify_url or not shopify_token:
                return self.transfer_error('Missing required parameters: shopify_url and shopify_token')
            
            # Initialize services
            shopify_service = ShopifyService(shopify_url, shopify_token, request.env)
            
            # Store service-specific credentials
            config = request.env['ir.config_parameter'].sudo()
            config.set_param('data_fetcher_shopify.shopify_url', shopify_url)
            config.set_param('data_fetcher_shopify.shopify_token', shopify_token)
            
            # Run the sync
            request.env['transfer.log'].fetch_all(shopify_service)
            return self.transfer_success()
            
        except Exception as e:
            _logger.exception("Error in Shopify sync: %s", str(e))
            return self.transfer_error(str(e))