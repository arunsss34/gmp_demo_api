import json
from db_connection import py_connection
from report.customer_po_rpt import last_updated_date_for_customer_po, last_updated_date_for_customer_po_lw1
from report.py_stock_rpt import last_updated_date_for_frame_wise_stock, last_updated_date_for_frame_wise_stock_lw


def get_outstanding_dp():
    try:
        qry = "SELECT day_pk,day FROM reporting.ageing_configuration WHERE is_active in (1,2) order by day_pk asc"
        res, k = py_connection.get_result_col(qry)
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return {"outstanding_dp": lst, "download_type": download_type()}
        else:
            return {"outstanding_dp": lst, "download_type": download_type()}
    except Exception as e:
        print(str(e))
        return {"outstanding_dp": [], "download_type": []}

def download_type():
    try:
        qry = "select download_pk, download_type from Reporting.download_type where is_active = 1"
        res, k = py_connection.get_result_col(qry)
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return lst
        else:
            return lst
    except Exception as e:
        print(str(e))
        return []

def get_document_type(decoded):
    try:
        qry = "select distinct(DocumentType) from RptSPG.salesreport where CompanyID =" + str(decoded.get('comp_fk'))
        res, k = py_connection.get_result_col(qry)
        lst = []
        b = {"DocumentType": 'Top 10'}
        lst.append(b)
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return {"document_type": lst}
        else:
            return {"document_type": lst}
    except Exception as e:
        print(str(e))
        return {"document_type": []}


def dates():
    try:
        qry = "select date_description,start_date,end_date from Reporting.dates where is_active = 1 order by [order] asc"
        res, k = py_connection.get_result_col(qry)
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return {"dates": lst}
        else:
            return {"dates": lst}
    except Exception as e:
        print(str(e))
        return {"dates": []}


def get_frame_last_updated_date(decoded):
    try:
        qry = ("SELECT TOP 1 CAST(updateddate AS DATE) AS last_updated_date FROM rptspg.YarnFrameWiseStock WHERE "
               "CompanyId = '{0}' ORDER BY updateddate DESC").format(decoded['comp_fk'])
        res = py_connection.get_result(qry)
        return {"dates": res[0][0], "last_updated_date": last_updated_date_for_frame_wise_stock(decoded)}
    except Exception as e:
        print(str(e))
        return {"dates": [], "last_updated_date": ''}

def customer_pending_order_dates(decoded):
    try:
        qry = ("select distinct(format(cast(DocumentDate as date), 'dd-MM-yyyy')) as date "
               "from rptspg.pendingorderreport where companyid ={0}").format(decoded['comp_fk'])
        res, k = py_connection.get_result_col(qry)
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return {"dates": lst, "last_updated_date": last_updated_date_for_customer_po(decoded)}
        else:
            return {"dates": lst, "last_updated_date": last_updated_date_for_customer_po(decoded)}
    except Exception as e:
        print(str(e))
        return {"dates": []}

def last_updated_date_for_customer_po_lw(decoded):
    try:
        qry = ("select distinct(format(cast(DocumentDate as date), 'dd-MM-yyyy')) as date "
               "from rptspg.pendingorderreport where companyid =1")
        res, k = py_connection.get_result_col(qry)
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return {"dates": lst, "last_updated_date": last_updated_date_for_customer_po_lw1()}
        else:
            return {"dates": lst, "last_updated_date": last_updated_date_for_customer_po_lw1()}

    except Exception as e:
        print(str(e))
        return {"dates": []}

def get_frame_last_updated_date_lw(decoded):
    try:
        qry = ("SELECT TOP 1 CAST(updateddate AS DATE) AS last_updated_date FROM rptspg.YarnFrameWiseStock WHERE "
               "CompanyId = 2 ORDER BY updateddate DESC")
        res = py_connection.get_result(qry)
        return {"dates": res[0][0], "last_updated_date": last_updated_date_for_frame_wise_stock_lw()}
    except Exception as e:
        print(str(e))
        return {"dates": [], "last_updated_date": ''}