from typing import Dict, Any, List, Optional
    
def map_account_to_partner(sf_account: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Account to Odoo Partner
    """
    # Map ownership to is_public
    ownership = sf_account.get("Ownership")
    is_public = "false"
    if ownership:
        is_public = "public" in (ownership.lower() or "")
    
    return {
        "name": sf_account.get("Name"),
        "phone": sf_account.get("Phone"),
        "website": sf_account.get("Website"),
        "ref": sf_account.get("Id"),
        "street": sf_account.get("BillingStreet"),
        "city": sf_account.get("BillingCity"),
        # "country_id/.id": sf_account.get("BillingCountry"),
        # "state_id/.id": sf_account.get("BillingState"),
        "zip": sf_account.get("BillingPostalCode"),
        "partner_latitude": sf_account.get("BillingLatitude"),
        "partner_longitude": sf_account.get("BillingLongitude"),
        # "industry_id/.id": sf_account.get("Industry"),
        "is_company": "true",
        "is_public": is_public,
        # Additional info can be stored in comment field or custom field
        "comment": sf_account.get("Description"),
    }

def map_contact_to_partner(sf_contact: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Contact to Odoo Partner
    """
    return {
        "name": f"{sf_contact.get('FirstName', '')} {sf_contact.get('LastName', '')}".strip(),
        "email": sf_contact.get("Email"),
        "phone": sf_contact.get("Phone"),
        "mobile": sf_contact.get("MobilePhone"),
        "ref": sf_contact.get("Id"),
        "function": sf_contact.get("Title"),  # Job title
        "type": 'contact',
        "street": sf_contact.get("MailingStreet"),
        "city": sf_contact.get("MailingCity"),
        # "country_id": sf_contact.get("MailingCountry"),
        # "country_id/name": "Australia",
        # "state_id/code": sf_contact.get("MailingState"),
        "zip": sf_contact.get("MailingPostalCode"),
        # "title/.id": sf_contact.get("Salutation"),
        "partner_latitude": sf_contact.get("MailingLatitude"),
        "partner_longitude": sf_contact.get("MailingLongitude"),
        "employee": "true",
        "is_company": "false",
        "company_type": "person",
        "lang": sf_contact.get("Languages__c"),
        "comment": sf_contact.get("Description"),
        "parent_id/.id": sf_contact.get("AccountId"),  # Used to link to parent company
    }

def map_lead_to_crm(sf_lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Lead to Odoo CRM Lead
    """
    return {
        "referred": sf_lead.get("Id"),
        "name": sf_lead.get("Company"),
        "type": "lead",
        "contact_name": f"{sf_lead.get('FirstName', '')} {sf_lead.get('LastName', '')}".strip(),
        "email_from": sf_lead.get("Email"),
        "phone": sf_lead.get("Phone"),
        "website": sf_lead.get("Website"),
        # title will be resolved by the sync process
        # source_id will be resolved by the sync process
        "active": "true", # always true to make record accessible
        # expected_revenue will be calculated based on AnnualRevenue if present
        "description": sf_lead.get("Description"),
    }

def map_opportunity_to_crm(sf_opportunity: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Opportunity to Odoo CRM Opportunity
    """
    return {
        "referred": sf_opportunity.get("Id"),
        "name": sf_opportunity.get("Name"),
        "type": "opportunity",
        # partner_id will be resolved by the sync process
        "date_deadline": sf_opportunity.get("CloseDate"),
        "expected_revenue": sf_opportunity.get("Amount", 0.0),
        "probability": sf_opportunity.get("Probability", 0.0),
        # stage_id will be resolved by the sync process
        "active": "true", # always true to make record accessible
        "ref": sf_opportunity.get("AccountId") 
    }

def map_sf_invoice_to_odoo(sf_invoice: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce blngInvoicec fields to Odoo account.move fields
    """
    # Calculate invoice status based on Salesforce status
    state = 'draft'
    if sf_invoice.get('blngInvoiceStatusc') == 'Posted':
        state = 'posted'
    elif sf_invoice.get('blngInvoiceStatusc') == 'Cancelled':
        state = 'cancel'
    
    # Calculate payment status based on Salesforce payment status
    payment_state = 'not_paid'
    if sf_invoice.get('blngPaymentStatusc') == 'Paid':
        payment_state = 'paid'
    elif sf_invoice.get('blngPaymentStatusc') == 'Partially Paid':
        payment_state = 'partial'
    
    return {
        "name": sf_invoice.get("Name"),  # Invoice number from Salesforce as reference
        "ref": sf_invoice.get("Id"),  # Store SF ID for future reference
        "move_type": "out_invoice",  # Customer invoice
        "state": state,
        "payment_state": payment_state,
        "invoice_date": sf_invoice.get("blng__InvoiceDate__c"),
        "invoice_date_due": sf_invoice.get("blng__DueDate__c"),
        "date": sf_invoice.get("blng__InvoiceDate__c"),  # Accounting date
        # Partner information - will need to be resolved during sync
        "ref": sf_invoice.get("blng__Account__c"),
        "amount_total": sf_invoice.get("blng__TotalAmount__c", 0.0),
        "amount_untaxed": sf_invoice.get("blng__Subtotal__c", 0.0),
        "amount_tax": sf_invoice.get("blng__TaxAmount__c", 0.0),
        "amount_residual": sf_invoice.get("blng__Balance__c", 0.0),
        "narration": sf_invoice.get("blng__Notes__c"),
        "sf_order_id": sf_invoice.get("blng__Order__c"),
    }

def map_invoice_line_to_odoo(sf_invoice_line: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Invoice Line (blng__InvoiceLine__c) to Odoo Invoice Line (account_move_line)
    """
    return {
        "matching_number": sf_invoice_line.get("Id"),
        "name": sf_invoice_line.get("Name", ""),
        "quantity": sf_invoice_line.get("blng__Quantity__c", 1.0),
        "price_unit": sf_invoice_line.get("blng__UnitPrice__c", 0.0),
        "price_subtotal": sf_invoice_line.get("blng__Subtotal__c", 0.0),
        "price_total": sf_invoice_line.get("blng__TotalAmount__c", 0.0),
        "balance": sf_invoice_line.get("blng__Balance__c", 0.0),
        "date": sf_invoice_line.get("blng__ChargeDate__c"),
        "date_maturity": sf_invoice_line.get("blng__DueDate__c"),
        "display_type": "product",  # Default value, may need mapping based on blng__ChargeType__c
        # Fields that will be resolved by the sync process
        "sf_invoice_id": sf_invoice_line.get("blng__Invoice__c"),
        "sf_product_id": sf_invoice_line.get("blng__Product__c"),
        # Additional fields that might be useful
        "tax_base_amount": sf_invoice_line.get("blng__TaxAmount__c", 0.0),
    }

def map_product_to_odoo(sf_product: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Product to Odoo Product
    """
    # product_type = "consu" if sf_product.get("Type") == "Goods" else "service"
    product_type = "consu" if sf_product.get("SBQQ__ChargeType__c") == "One-Time" else "service"
    pricebook_entries = sf_product.get("PricebookEntries", {}).get("records", [])
    list_price = pricebook_entries[0]["UnitPrice"] if pricebook_entries else 0.0

    return {
        "description": sf_product.get("Id"),
        "name": sf_product.get("Name", ""),
        "default_code": sf_product.get("ProductCode", ""),
        "description_sale": sf_product.get("Description", ""),
        "active": "true" if sf_product.get("IsActive") else "false",
        "type": product_type,
        "list_price": list_price
    }

def map_order_to_odoo(sf_order: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Order to Odoo Sale Order
    """
    # Map Salesforce status to Odoo state
    state = "sale" if sf_order.get("Status") == "Activated" else "sent"

    return {
        "reference": sf_order.get("Id"),
        "name": sf_order.get("OrderNumber", ""),
        "date_order": sf_order.get("EffectiveDate"),
        "client_order_ref": sf_order.get("PoNumber", ""),
        "amount_total": sf_order.get("TotalAmount", 0.0),
        "state": state,
        # partner_id will be resolved by the sync process
    }

def map_order_line_to_odoo(sf_order_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Salesforce Order Item to Odoo Sale Order Line
    """
    return {
        "virtual_id": sf_order_item.get("Id"),
        # "name": sf_order_item.get("Description", ""),
        "product_uom_qty": sf_order_item.get("Quantity", 1.0),
        "price_unit": sf_order_item.get("UnitPrice", 0.0),
        "price_subtotal": sf_order_item.get("TotalPrice", 0.0),
        # product_id will be resolved by the sync process
        # order_id will be resolved by the sync process
    }
