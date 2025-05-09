from typing import Dict, Any, List, Optional
# from ..models.salesforce import SalesforceAPI

class Query:
    @staticmethod
    def fetch_contacts() -> List[Dict[str, Any]]:
        """
        Fetch contacts from Salesforce
        """
        query = """
        SELECT Id, Name, BillingStreet, BillingCity, BillingState, 
               BillingPostalCode, BillingCountry, BillingLatitude, BillingLongitude, 
               Phone, Website, Industry, Ownership, Description, Active__c 
        FROM Account
        """
        # removed billingstatecode
        # salesforce = SalesforceAPI()
        # return salesforce.query(query)
        return query
    
    @staticmethod
    def fetch_employees() -> List[Dict[str, Any]]:
        """
        Fetch employees from Salesforce
        """
        query = """
        SELECT Id, FirstName, LastName, AccountId, Email, Phone, MobilePhone, 
               Title, MailingStreet, MailingCity, MailingState, 
               MailingCountry, MailingPostalCode, MailingLatitude, MailingLongitude, 
               Languages__c, Salutation, Description
        FROM Contact
        """
        # removed mailingstatecode
        # return SalesforceAPI.query(query)
        return query
    
    @staticmethod
    def fetch_leads() -> List[Dict[str, Any]]:
        """
        Fetch leads from Salesforce
        """
        query = """
        SELECT Id, Salutation, FirstName, LastName, Status, Company, Email, 
               Phone, Website, LeadSource, AnnualRevenue, OwnerId, 
               CreatedDate, LastModifiedDate 
        FROM Lead
        """
        # return SalesforceAPI.query(query)
        return query
    
    @staticmethod
    def fetch_opportunities() -> List[Dict[str, Any]]:
        """
        Fetch opportunities from Salesforce
        """
        query = """
        SELECT Id, Name, StageName, CloseDate, Amount, AccountId, OwnerId, 
               CreatedDate, LastModifiedDate
        FROM Opportunity
        """
        # return SalesforceAPI.query(query)
        return query
    
    @staticmethod
    def fetch_invoices() -> List[Dict[str, Any]]:
        """
        Fetch invoices (blngInvoicec) from Salesforce
        """
        query = """
            SELECT Id, Name, blng__Account__c, blng__InvoiceDate__c, blng__DueDate__c, blng__InvoiceStatus__c, 
                blng__PaymentStatus__c, blng__TotalAmount__c, blng__Subtotal__c, blng__TaxAmount__c, blng__Balance__c, 
                blng__Notes__c, blng__Order__c, blng__TargetDate__c, CreatedDate, LastModifiedDate
            FROM blng__Invoice__c
        """
        # return SalesforceAPI.query(query)
        return query

    @staticmethod
    def fetch_invoice_lines() -> List[Dict[str, Any]]:
        """
        Fetch invoice lines for a specific invoice
        """
        query = """
            SELECT Id, Name, blng__Invoice__c, blng__Product__c, blng__Quantity__c, blng__UnitPrice__c, blng__TotalAmount__c, 
            blng__TaxAmount__c 
            FROM blng__InvoiceLine__c
        """
        # return SalesforceAPI.query(query)
        return query

    @staticmethod
    def fetch_products() -> List[Dict[str, Any]]:
        """
        Fetch products from Salesforce
        """
        query = """
        SELECT Id, Name, ProductCode, Description, IsActive, Family, 
               StockKeepingUnit, SBQQ__ChargeType__c 
        FROM Product2
        """
        # return SalesforceAPI.query(query)
        return query
        
    @staticmethod
    def fetch_product_prices() -> List[Dict[str, Any]]:
        """
        Fetch product prices from Salesforce
        """
        query = """
        SELECT Id, Product2Id, UnitPrice, Pricebook2Id, IsActive 
        FROM PricebookEntry 
        WHERE IsActive = TRUE
        """
        # return SalesforceAPI.query(query)
        return query
    
    @staticmethod
    def fetch_orders() -> List[Dict[str, Any]]:
        """
        Fetch orders from Salesforce
        """
        query = """
        SELECT Id, AccountId, Status, OrderNumber, EffectiveDate, PoNumber, 
               TotalAmount, BillingStreet, BillingCity, BillingState, 
               BillingPostalCode, BillingCountry, ShippingStreet, 
               ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry
        FROM Order
        """
        # return SalesforceAPI.query(query)
        return query
    
    @staticmethod
    def fetch_order_lines() -> List[Dict[str, Any]]:
        """
        Fetch order items from Salesforce
        """
        query = """
        SELECT Id, OrderId, Product2Id, Quantity, UnitPrice, TotalPrice, 
               ListPrice, ServiceDate, Description, CreatedDate, OrderItemNumber 
        FROM OrderItem
        """
        # return SalesforceAPI.query(query)
        return query
