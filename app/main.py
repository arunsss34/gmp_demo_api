# Version 1 -----> V1
from starlette.middleware.cors import CORSMiddleware
from uvicorn import run
from fastapi import FastAPI, Request, Depends, HTTPException
from auth import py_jwt
from dashboard import py_dashboard
from generate_pdf import outstanding_pdf, stock_pdf, party_outstanding_pdf
from graph import py_graph
from login import py_login
from filter import py_filter
from report import py_report, py_stock_rpt, customer_po_rpt, py_party_outstanding
from user import py_user
from main_menu import py_main_menu
from sales_analysis import py_sales_analysis
import json

auth_scheme = py_jwt.JWTBearer()

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "success"}


@app.post("/bis/login")
# V1 -- Login Api for bis
async def login(request: Request):
    try:
        request = await request.json()
        response = py_login.login(request)
        return response
    except Exception as e:
        print(str(e))


@app.get("/bis/main_menu")
# V1 -- Sidebar Dynamic
async def main_menu(decoded=Depends(auth_scheme)):
    if decoded:
        try:
            response = py_main_menu.main_menu(decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/gmp/get_gmpapiipconfig")
async def get_gmpapiipconfig():
        try:
            response = py_filter.fn_get_gmpapiipconfig()
            return response
        except Exception as e:
            print(str(e))


@app.get("/bis/get_user_list")
# V1 -- getting user list
async def get_user_list(decoded=Depends(auth_scheme)):
    if decoded:
        try:
            response = py_user.get_user_list(decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_role")
# V1 -- getting role dropdown
async def get_role(decoded=Depends(auth_scheme)):
    if decoded:
        try:
            response = py_user.get_role()
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.post("/bis/add_user")
# V1 -- adding user for each company
async def add_user(request: Request, decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = await request.json()
            response = py_user.add_user(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.post("/bis/edit_user")
# V1 -- editing user for each company
async def edit_user(request: Request, decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = await request.json()
            response = py_user.edit_user(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.post("/bis/delete_user")
# V1 -- deleting user for each company
async def delete_user(request: Request, decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = await request.json()
            response = py_user.delete_user(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

# <---------------------------- Outstanding Receivables Api starts here ----------------------------------->
@app.get("/bis/get_agent_details")
# V1 ----> getting agent details
async def get_agent_details(data: str = '{"menu_pk": 3}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_report.get_report_table(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_party_info")
# V1 ----> getting party info
async def get_party_info(data: str = '{"menu_pk": 3, "agent_id": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_report.get_party_info_by_agent_id(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_party_details_by_agent")
# V1 ----> getting party details with agent id
async def get_party_details_by_agent(data: str = '{"menu_pk": 3, "agent_id": 1, "customer_id": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            print(request,"444")
            response = py_report.get_party_details_by_agent_id(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_agent_graph")
# V1 ----> getting agent graph
async def get_agent_graph(data: str = '{"menu_pk": 3}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_graph.get_agent_graph(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_each_agent_graph")
# V1 ----> getting each_agent_graph
async def get_each_agent_graph(data: str = '{"menu_pk": 3, "agent_id": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_graph.get_each_agent_graph(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_each_party_graph")
# V1 ----> getting each_party_graph
async def get_each_party_graph(data: str = '{"menu_pk": 3, "agent_id": 1, "customer_id":1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_graph.get_each_party_graph(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_outstanding_dp")
async def get_outstanding_dp(decoded=Depends(auth_scheme)):
    if decoded:
        try:
            response = py_filter.get_outstanding_dp()
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_outstanding_overview_pdf")
async def get_outstanding_overview_pdf(data: str = '{"menu_pk": 3, "download_pk": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = outstanding_pdf.get_outstanding_overview_pdf(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_outstanding_party_overview_pdf")
async def get_outstanding_party_overview_pdf(data: str = '{"menu_pk": 3, "download_pk": 1, "agent_id":1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = outstanding_pdf.get_outstanding_party_overview_pdf(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

@app.get("/bis/get_outstanding_party_details_overview_pdf")
async def get_outstanding_party_details_overview_pdf(data: str = '{"menu_pk": 3, "agent_id":1, "customer_id": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = outstanding_pdf.get_outstanding_party_details_overview_pdf(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


# -------------------------------------------------------------> Stock Report

# Lucky Yarns Stock Report
@app.get("/bis/get_frame_last_update_dt")
# V1 ----> getting document type
async def get_frame_last_update_dt(decoded=Depends(auth_scheme)):
    if decoded:
        try:
            response = py_filter.get_frame_last_updated_date(decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

@app.get("/bis/get_stock_details")
# V1 ----> getting stock  details
async def get_stock_details(data: str = '{"menu_pk": 4, "type": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_stock_rpt.get_stock_report_table(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

@app.get("/bis/get_stock_pdf")
# V1 ----> getting stock pdf
async def get_stock_pdf(data: str = '{"menu_pk": 4, "type": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = stock_pdf.generate_stock_pdf(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

# Lucky Weaves Stock Report
@app.get("/bis/get_frame_last_update_dt_lw")
# V1 ----> getting document type
async def get_frame_last_update_dt_lw(decoded=Depends(auth_scheme)):
    if decoded:
        try:
            response = py_filter.get_frame_last_updated_date_lw(decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

@app.get("/bis/get_stock_details_lw")
# V1 ----> getting stock  details
async def get_stock_details_lw(data: str = '{"menu_pk": 4, "type": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_stock_rpt.get_stock_report_table_lw(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

@app.get("/bis/get_stock_pdf_lw")
# V1 ----> getting stock pdf
async def get_stock_pdf_lw(data: str = '{"menu_pk": 4, "type": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = stock_pdf.generate_stock_pdf(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


# -----------------------------------------------> dashboard Api

@app.get("/bis/get_document_type")
# V1 ----> getting document type
async def get_document_type(decoded=Depends(auth_scheme)):
    if decoded:
        try:
            response = py_filter.get_document_type(decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/sales_type_chart")
# V1 ----> getting sales type chart
async def sales_type_chart(data: str = '{"menu_pk": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_dashboard.sales_type_chart(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

@app.get("/bis/get_sales_document_type_chart")
# V1 ----> getting sales document type chart
async def get_sales_document_type_chart(data: str = '{"menu_pk": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_dashboard.sales_document_type_chart(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_sales_customer_chart")
# V1 ----> getting sales customer chart
async def get_sales_customer_chart(data: str = '{"menu_pk": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_dashboard.sales_customer_chart(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


# --------------------> Sales Analysis Document type chart

@app.get("/bis/get_dates")
# V1 ----> getting dates for charts
async def get_dates(decoded=Depends(auth_scheme)):
    if decoded:
        try:
            response = py_filter.dates()
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_sales_analysis_document_type_chart")
# V1 ----> getting sales Analysis document chart
async def get_sales_customer_chart(data: str = '{"menu_pk": 1, "start_date": '', "end_date": '', "year": 2024}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_sales_analysis.get_sales_document_type(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_sales_receipt_charts")
# V1 ----> getting sales Analysis document chart
async def get_sales_receipt_charts(data: str = '{"menu_pk": 1, "start_date": '', "end_date": '', "year": 2024}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            print(request,"11")
            response = py_sales_analysis.get_sales_chart(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_sales_customer_pie_chart")
# V1 ----> getting sales Analysis type pie chart
async def get_sales_customer_pie_chart(data: str = '{}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            print(request,"11")
            response = py_sales_analysis.get_sales_customer_pie_chart(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_sales_analysis_type_chart")
# V1 ----> getting sales Analysis type pie chart
async def get_sales_analysis_type_chart(data: str = '{}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            print(request,"11")
            response = py_sales_analysis.get_sales_type_chart(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

# ----------------------------------------------------------->  Customer Order PO report

# Lucky Yarns Customer PO
@app.get("/bis/customer_pending_order_dates")
# V1 ----> getting dates for customer_pending_order
async def customer_pending_order_dates(decoded=Depends(auth_scheme)):
    if decoded:
        try:
            response = py_filter.customer_pending_order_dates(decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_customer_po_details")
# V1 ----> getting customer po details
async def get_customer_po_details(data: str = '{"date":''}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            print(request,"11")
            response = customer_po_rpt.get_customer_po_details(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_customer_po_pdf")
# V1 ----> getting customer po details
async def get_customer_po_pdf(data: str = '{"date":''}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            print(request,"11")
            response = customer_po_rpt.get_customer_po_pdf(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_pending_customer_po_details")
# V1 ----> getting customer po details
async def get_pending_customer_po_details(data: str = '{"date":''}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = customer_po_rpt.get_customer_po_details(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


# Lucky Weaves Customer PO details
@app.get("/bis/customer_pending_order_dates_lw")
# V1 ----> getting dates for customer_pending_order
async def customer_pending_order_dates_lw(decoded=Depends(auth_scheme)):
    if decoded:
        try:
            response = py_filter.last_updated_date_for_customer_po_lw(decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_customer_po_details_lw")
# V1 ----> getting customer po details
async def get_customer_po_details_lw(data: str = '{"date":''}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = customer_po_rpt.get_customer_po_details_lw(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

@app.get("/bis/get_customer_po_pdf_lw")
# V1 ----> getting customer po details
async def get_customer_po_pdf_lw(data: str = '{"date":''}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            print(request, '009')
            response = customer_po_rpt.get_customer_po_pdf_lw(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


# <------------------------------ Party wise Outstanding details Api starts here----------------------------------------->

@app.get("/bis/party_otd_get_agent_details")
# V1 ----> getting agent details
async def party_otd_get_agent_details(data: str = '{"menu_pk": 3}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_party_outstanding.get_report_table(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

@app.get("/bis/party_otd_get_party_info")
# V1 ----> getting party wise outstanding info
async def party_otd_get_party_info(data: str = '{"menu_pk": 3, "customer_id": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_party_outstanding.get_party_info_by_agent_id(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

@app.get("/bis/party_otd_get_party_details_by_agent")
# V1 ----> getting party details with agent id
async def party_otd_get_party_details_by_agent(data: str = '{"menu_pk": 3, "agent_id": 1, "customer_id": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = py_party_outstanding.get_party_details_by_agent_id(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


# pdf party wise

@app.get("/bis/party_get_outstanding_overview_pdf")
async def party_get_outstanding_overview_pdf(data: str = '{"menu_pk": 3, "download_pk": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = party_outstanding_pdf.get_outstanding_overview_pdf(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


@app.get("/bis/get_party_outstanding_party_overview_pdf")
async def get_party_outstanding_party_overview_pdf(data: str = '{"menu_pk": 3, "download_pk": 1, "customer_id":1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = party_outstanding_pdf.get_outstanding_party_overview_pdf(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")

@app.get("/bis/get_party_outstanding_party_details_overview_pdf")
async def get_party_outstanding_party_details_overview_pdf(data: str = '{"menu_pk": 3, "agent_id":1, "customer_id": 1}', decoded=Depends(auth_scheme)):
    if decoded:
        try:
            request = json.loads(data)
            response = party_outstanding_pdf.get_outstanding_party_details_overview_pdf(request, decoded)
            return response
        except Exception as e:
            print(str(e))
    else:
        raise HTTPException(status_code=401, detail="Invalid token or expired token.")


if __name__ =="__main__":
    run(app, host='0.0.0.0', port=810)
