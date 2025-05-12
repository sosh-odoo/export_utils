from odoo import http
from odoo.http import request
import logging
from ..utils.salesforce import SalesforceAPI
from odoo.addons.data_fetcher_base.controllers.main import SyncController as BaseSyncController # type: ignore

logger = logging.getLogger(__name__)

class SalesforceSync(BaseSyncController):

    def _handle_salesforce(self, data):
        """Handle Salesforce data transfer"""
        salesforce_creds = {
            'client_id': data.get('sf_client_id'),
            'client_secret': data.get('sf_client_secret'),
            'username': data.get('sf_username'),
            'password': data.get('sf_password'),
            'security_token': data.get('sf_security_token'),
        }
        
        # Store service-specific credentials if needed
        config = request.env['ir.config_parameter'].sudo()
        for key, value in salesforce_creds.items():
            if value:
                config.set_param(f'data_fetcher_salesforce.{key}', value)
                
        # Initialize API
        sf_api = SalesforceAPI(salesforce_creds)
        if sf_api.authenticate():
            logger.info("Salesforce API authenticated successfully.")
            request.env['transfer.log'].sync_all(sf_api)
            return self.transfer_success()
        else:
            logger.error("Salesforce API authentication failed.")
            return self.transfer_error("Salesforce authentication failed")
        