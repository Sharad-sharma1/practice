#!/usr/bin/env python3
import sys, re, smtplib, os
import numpy as np, pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
import snowflake.connector
import matplotlib.pyplot as plt
# from credentials import sender_email, to_email,cc_email, app_password, user, password, account, server
from email_bulk_all_queries import DashBoardBulkQuriesOfTable as dbb_tq, DashBoardBulkQuriesOfCharts as dbb_cq
user='avdhutG'
password='Powai@1234'
account='PHARMARACK-ANALYTICS'
sender_email = "pranalytics.support@pharmarack.com"
app_password= 'Yoc28153'
server='smtp.office365.com'
# to_email = ["swardaj@pharmarack.com","amit.kaul@pharmarack.com", "Vishakha.shah@pharmarack.com","avdhut.ghatage@pharmarack.com"]
# cc_email = ["aarifp@pharmarack.com", "syedm@pharmarack.com", "Sujatag@pharmarack.com", "Hiren.Karnani@pharmarack.com", "Surya.Choudhury@pharmarack.com", "suryas@pharmarack.com", "ramasamy.ramar@pharmarack.com", "shrutika.wagh@pharmarack.com", "poojag@pharmarack.com", "isha.rathi@pharmarack.com", "Dhruv.Gulati@pharmarack.com"]

to_email = ["swardaj@pharmarack.com", "aarifp@pharmarack.com"]
cc_email = ["sharad.sharma@pharmarack.com", "avdhut.ghatage@pharmarack.com", "animesh.oze@pharmarack.com"]

# "swardaj@pharmarack.com","amit.kaul@pharmarack.com", "Vishakha.shah@pharmarack.com"
# "aarifp@pharmarack.com", "syedm@pharmarack.com", "Sujatag@pharmarack.com", "Hiren.Karnani@pharmarack.com", "Surya.Choudhury@pharmarack.com", "suryas@pharmarack.com", "ramasamy.ramar@pharmarack.com", "shrutika.wagh@pharmarack.com", "poojag@pharmarack.com", "isha.rathi@pharmarack.com", "Dhruv.Gulati@pharmarack.com"

from datetime import datetime, timedelta
to_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
from_date = None
if from_date:
    from_date = from_date.strftime('%Y-%m-%d')
else:
    from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

daterange_replace_str_more_then_one_day = f" BETWEEN '{from_date}' AND '{to_date}'"
daterange_replace_str_one_day = f" BETWEEN '{to_date}' AND '{to_date}'"

image_main_path = "./dashboard_images_for_mail/"
if not os.path.exists(image_main_path):
    os.makedirs(image_main_path)

csv_main_path = "./dashboard_chart_df_csv/"
if not os.path.exists(csv_main_path):
    os.makedirs(csv_main_path)



# Connect to Snowflake using a context manager
def fetch_data_from_snowflake(query, return_columns=False):
    with snowflake.connector.connect(user=user, password=password, account=account) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            if return_columns:
                return cursor.fetchall(), [desc[0] for desc in cursor.description]
            else:
                return cursor.fetchall()

# Function to format numbers with commas
def format_number(value):
    return "{:,}".format(value)

# Function to generate an HTML table from query results
def generate_html_table(columns, data, title, extra_info=""):
    try:
        table_html = f"""
        <html>
        <body>
            <p>{title}</p>  <!-- Title added here -->
            <p>{extra_info}</p>
            <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%; text-align: center;">
            <thead>
                <tr style="background-color: #f2f2f2; font-weight: bold;">
        """
        if not data:
            table_html += "<html><body><p>No data for yesterday</p></body></html>"
        else:
            # Add headers
            for column in columns:
                table_html += f"<th style='border: 1px solid black;'>{column}</th>"
        
        table_html += "</tr></thead><tbody>"

        # Add rows
        for row in data:
            table_html += "<tr>"
            for cell in row:
                table_html += f"<td style='border: 1px solid black;'>{cell}</td>"
            table_html += "</tr>"
        
        table_html += "</tbody></table></body></html>"
        return table_html
    except Exception as e:
        print(f'------------ error for {title}')
        print(e)
        print(f'------------ error for {title}')


def bulk_update_query():
    # BULK MAPPING DATA QUERY 1
    query_1 = """
    SELECT TO_DATE(A.RECORD_DATE) RECORD_DATE,
        B.USERTYPE,
        COUNT(DISTINCT CASE WHEN CURRENT_ALLOCATION='Bulk Mapping' THEN USERNAME END) AS USERS_COUNT,
        COUNT(CASE WHEN CURRENT_ALLOCATION='Bulk Mapping' THEN DISTRIBUTOR_CODE END) AS BULK_MAPPED_COUNT,
        COUNT(DISTINCT CASE WHEN CURRENT_ALLOCATION='Bulk Mapping' THEN MDM_PRODUCT_CODE END) AS BULK_MAPPED_PRODUCTS,
        ROUND((BULK_MAPPED_COUNT / REPLACE(USERS_COUNT, 0, 1))) MAPPING_PER_USER,
        ROUND((BULK_MAPPED_PRODUCTS / REPLACE(USERS_COUNT, 0, 1))) PRODUCT_PER_USER
    FROM (
        SELECT DISTRIBUTOR_CODE, PRODUCT_CODE, PRODUCT_NAME, PACK, REGEX, MDM_PRODUCT_CODE, CREATED_BY, RECORD_DATE
        FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_MAPPING
        UNION ALL
        SELECT DISTRIBUTOR_CODE, PRODUCT_CODE, PRODUCT_NAME, PACK, REGEX, MDM_PRODUCT_CODE, CREATED_BY, RECORD_DATE
        FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_UPLOAD
    ) A
    INNER JOIN SANDBOX_DB.SANDBOX_ANALYST.AVDHUT_18DEC_DATA_STEWARDS B 
        ON A.CREATED_BY = B.USERNAME AND TO_DATE(A.RECORD_DATE) = B.RECORD_DATE
    WHERE TO_DATE(A.RECORD_DATE) = DATEADD(DAY, -1, CURRENT_DATE)
    GROUP BY ALL
    ORDER BY TO_DATE(A.RECORD_DATE) DESC, USERTYPE;
    """

    data1 = fetch_data_from_snowflake(query_1)
    columns1 = ['RECORD_DATE', 'USERTYPE', 'USERS_COUNT', 'BULK_MAPPED_COUNT', 'BULK_MAPPED_PRODUCTS', 'MAPPING_PER_USER', 'PRODUCT_PER_USER']
    df1 = pd.DataFrame(data1, columns=columns1)

    # BULK MAPPING DATA QUERY 2
    query_2 = """
    SELECT 
        COUNT(CASE WHEN CURRENT_ALLOCATION='Bulk Mapping' THEN DISTRIBUTOR_CODE END) AS BULK_MAPPED_COUNT,
        COUNT(DISTINCT CASE WHEN CURRENT_ALLOCATION='Bulk Mapping' THEN MDM_PRODUCT_CODE END) AS BULK_MAPPED_PRODUCTS
    FROM (
        SELECT DISTRIBUTOR_CODE, PRODUCT_CODE, PRODUCT_NAME, PACK, REGEX, MDM_PRODUCT_CODE, CREATED_BY, RECORD_DATE
        FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_MAPPING
        UNION ALL
        SELECT DISTRIBUTOR_CODE, PRODUCT_CODE, PRODUCT_NAME, PACK, REGEX, MDM_PRODUCT_CODE, CREATED_BY, RECORD_DATE
        FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_UPLOAD
    ) A
    INNER JOIN SANDBOX_DB.SANDBOX_ANALYST.AVDHUT_18DEC_DATA_STEWARDS B 
        ON A.CREATED_BY = B.USERNAME AND TO_DATE(A.RECORD_DATE) = B.RECORD_DATE
    WHERE TO_DATE(A.RECORD_DATE) >= '2024-12-07'  
        AND TO_DATE(A.RECORD_DATE) <= DATEADD(DAY, -1, CURRENT_DATE)
    GROUP BY ALL;
    """

    data1A = fetch_data_from_snowflake(query_2)
    columns1A = ['BULK_MAPPED_COUNT', 'BULK_MAPPED_PRODUCTS']
    df1A = pd.DataFrame(data1A, columns=columns1A)

    # Define the formatted values
    mapped_count_til_date = format_number(df1A.iloc[0]['BULK_MAPPED_COUNT'])
    unique_product_mapped_til_date = format_number(df1A.iloc[0]['BULK_MAPPED_PRODUCTS'])

    # HTML for BULK MAPPING REPORT
    table_html_1 = generate_html_table(
        columns1, 
        data1, 
        "Hi All,<br><br>Please find the daily dashboard for mapping, <br>", 
        f"Mapped Count till Date: <strong>{mapped_count_til_date}</strong><br>Unique Products Mapped till Date: <strong>{unique_product_mapped_til_date}</strong>"
    )

    # BULK QC DATA QUERY
    query_3 = """
    SELECT TO_DATE(RECORD_DATE) RECORD_DATE,
        COUNT(DISTINCT QCED_BY) USER_COUNT,
        COUNT(1) TOTAL_QCED,
        COUNT(CASE WHEN QC_FLAG=1 THEN 1 END) QC_PASSED,
        COUNT(CASE WHEN QC_FLAG=2 THEN 1 END) QC_FAILED
    FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_AUDIT_QC
    WHERE TO_DATE(RECORD_DATE) = DATEADD(DAY, -1, CURRENT_DATE)
    GROUP BY ALL
    ORDER BY TO_DATE(RECORD_DATE) DESC;
    """

    data2 = fetch_data_from_snowflake(query_3)
    columns2 = ['RECORD_DATE', 'USER_COUNT', 'TOTAL_QCED', 'QC_PASSED', 'QC_FAILED']
    df2 = pd.DataFrame(data2, columns=columns2)

    if data1 == data2:
        print("Data1 and Data2 are the same. Terminating the script.")
        sys.exit()

    # BULK QC TILL DATE QUERY
    query_4 = """
    SELECT
        COUNT(1) TOTAL_QCED,
        COUNT(CASE WHEN QC_FLAG=1 THEN 1 END) QC_PASSED,
        ROUND((QC_PASSED / TOTAL_QCED) * 100) AS QC_PASSED_PERCENTAGE
    FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_AUDIT_QC
    WHERE TO_DATE(RECORD_DATE) < CURRENT_DATE;
    """

    data2A = fetch_data_from_snowflake(query_4)
    columns2A = ['TOTAL_QCED', 'QC_PASSED', 'QC_PASSED_PERCENTAGE']
    df2A = pd.DataFrame(data2A, columns=columns2A)

    # Define the formatted values
    qced_count_til_date = format_number(df2A.iloc[0]['TOTAL_QCED'])
    qc_passed_percentage_til_date = format_number(df2A.iloc[0]['QC_PASSED_PERCENTAGE'])

    # HTML for BULK QC REPORT
    table_html_2 = generate_html_table(
        columns2, 
        data2, 
        "<strong>Bulk QC Report:</strong>", 
        f"QCED Count till Date: <strong>{qced_count_til_date}</strong><br>QC Passed Percentage till Date: <strong>{qc_passed_percentage_til_date}%</strong>"
    )
    return table_html_1, table_html_2

def send_email(subject, body, image_path_list:list=None):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(to_email)
    msg['Cc'] = ", ".join(cc_email)
    msg['Subject'] = subject
    msg['In-Reply-To'] = '<PN3P287MB2779CA41DD373A039733C5789CE62@PN3P287MB2779.INDP287.PROD.OUTLOOK.COM>'
    msg['References'] = '<PN3P287MB2779CA41DD373A039733C5789CE62@PN3P287MB2779.INDP287.PROD.OUTLOOK.COM>'

    # Attach the body content
    msg.attach(MIMEText(body, 'html'))

    # If an image path is provided, attach the image
    if image_path_list:
        for index, image_path in enumerate(image_path_list):
            try:
                with open(image_path, 'rb') as img_file:
                    img = MIMEImage(img_file.read())
                    img.add_header('Content-ID', f'<image{index}>')  # You can reference this in your HTML body
                    msg.attach(img)
            except Exception as e:
                print(f"Error attaching image: {e}")

    recipients = to_email + cc_email

    try:
        with smtplib.SMTP(server, 587) as s:
            s.starttls()
            s.login(sender_email, app_password)
            s.sendmail(sender_email, recipients, msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")

def snowflake_data_generate_html(query:str, section_name:str, date_range_html_tag:str="", for_table:bool=False, data_set=None, column_names=None):
    if not data_set and not column_names:
        data_set, column_names = fetch_data_from_snowflake(query, return_columns=True) 
    # df = pd.DataFrame(data_set, columns=column_names)
    # match = re.search(r'<h4>(.*?):<\/h4>', section_name).group(1).strip().replace(" ", "_")
    # print('matchhhhhhhhh', match)
    # csv_path = csv_main_path+f"{match}.csv"
    # print('csv_path', match)
    # df.to_csv(csv_path)
    # print('=========', df)
    # Read CSV

    if for_table:
        # print(f'-------------{section_name}------')
        # print(data_set)
        # print(f'-------------{section_name}------')
        table_html = generate_html_table(
                column_names, 
                data_set, 
                section_name, 
                date_range_html_tag
            )
        return table_html, data_set, column_names
    return data_set, column_names

def daily_dashboard_before_chart_table():    

    obj_table_query = dbb_tq()
    
    query_daily_dashboard = re.sub(r"\s*=\s*:\s*daterange", f"{daterange_replace_str_one_day}", obj_table_query.query_string_daily_dashboard)
    # print('***************************** before exceute (query_daily_dashboard) ******************************************')
    # print(query_daily_dashboard)
    # print('***************************** before exceute (query_daily_dashboard) ******************************************')
    section_detail = "<p>Hi All,<br><br>Please find the daily dashboard for mapping, <br></p> <h4>Project wise daily stats:</h4>"
    # date_range = f"From Date: <strong>{from_date}</strong> - Till Date: <strong>{to_date}</strong>"
    table_html_daily_dashboard = snowflake_data_generate_html(query_daily_dashboard, section_detail, for_table=True)[0]
    
    # ------------------------------------------------------- Daily Dashboard End ----------------------------------------------- #

    query_manual_mapping = re.sub(r"\s*=\s*:\s*daterange", f"{daterange_replace_str_one_day}", obj_table_query.query_string_manual_mapping)
    # print('***************************** before exceute (query_manual_mapping) ******************************************')
    # print(query_manual_mapping)
    # print('***************************** before exceute (query_manual_mapping) ******************************************')
    table_html_manual_mapping = snowflake_data_generate_html(query_manual_mapping, "<h4>Manual mapping stats:</h4>", for_table=True)[0]

    # ------------------------------------------------------- Manual Mapping End ----------------------------------------------- #

    query_active_user = re.sub(r"\s*=\s*:\s*daterange", f"{daterange_replace_str_one_day}", obj_table_query.query_string_active_user)
    # print('***************************** before exceute (query_active_user) ******************************************')
    # print(query_active_user)
    # print('***************************** before exceute (query_active_user) ******************************************')
    data_set1 = fetch_data_from_snowflake(query_active_user) 

    query_avg_active_user = re.sub(r"\s*=\s*:\s*daterange", f"{daterange_replace_str_more_then_one_day}", obj_table_query.query_string_avg_active_user)
    # print('***************************** before exceute (query_avg_active_user) ******************************************')
    # print(query_avg_active_user)
    # print('***************************** before exceute (query_avg_active_user) ******************************************')
    data_set2 = fetch_data_from_snowflake(query_avg_active_user) 

    col_name = ("YESTERDAY'S ACTIVE USERS", f"AVERAGE ACTIVE USERS ({from_date} TO {to_date})")
    data_s = ([data_set1[-1][-1], data_set2[-1][-1]],)

    table_html_active_user_detail = snowflake_data_generate_html(query_active_user, "<h4>Active User Detail:</h4>", 
                                                                 for_table=True, column_names=col_name, data_set=data_s)[0]

    # ------------------------------------------------------- Active User Detail End ----------------------------------------------- #

    query_performer_for_selected_range = re.sub(r"\s*=\s*:\s*daterange", f"{daterange_replace_str_more_then_one_day}", obj_table_query.query_string_performer_for_selected_range)
    # print('***************************** before exceute (query_performer_for_selected_range) ******************************************')
    # print(query_performer_for_selected_range)
    date_range = f"From Date: <strong>{from_date}</strong> - Till Date: <strong>{to_date}</strong>"
    # print('***************************** before exceute (query_performer_for_selected_range) ******************************************')
    table_html_performer_for_selected_range = snowflake_data_generate_html(query_performer_for_selected_range, "<h4>Star performer for manual efforts:</h4>", date_range_html_tag=date_range, for_table=True)[0]

    # ------------------------------------------------------- Performer For Selected Range ----------------------------------------------- #
    # print("----------------table_html_daily_dashboard--------------------")
    # print(table_html_daily_dashboard)
    # print("----------------table_html_manual_mapping--------------------")
    # print(table_html_manual_mapping)
    # print("----------------table_html_active_user_detail--------------------")
    # print(table_html_active_user_detail)
    # print("----------------table_html_performer_for_selected_range--------------------")
    # print(table_html_performer_for_selected_range)
    set_of_html_table = table_html_daily_dashboard + table_html_manual_mapping + \
                        table_html_active_user_detail + table_html_performer_for_selected_range

    return set_of_html_table

def line_graph_function(line_graph_name:str, col_name_uniq:str, x_axis_col_name:str, y_axis_col_name:str, line_graph_query:str):
    try:
        query_for_line_graph = re.sub(r"\s*=\s*:\s*daterange", f"{daterange_replace_str_more_then_one_day}", line_graph_query)
        # print(f'***************************** before exceute (query_{line_graph_name}) ******************************************')
        # print(query_for_line_graph)
        # print(f'***************************** before exceute (query_{line_graph_name}) ******************************************')

        data_set, column_names = snowflake_data_generate_html(query_for_line_graph, f"line_graph_name")
        df = pd.DataFrame(data_set, columns=column_names)

        # csv_path = csv_main_path+f"{line_graph_name}.csv"
        # df.to_csv(csv_path)
        # print('=========', df)
        # # Read CSV
        # df = pd.read_csv(csv_path)

        each_data = df[col_name_uniq].unique()
        df.sort_values(by=x_axis_col_name)
        # Dictionary to store the datasets
        data_set_lists = {}
        for i in each_data:
            filtered_df = df.loc[df[col_name_uniq] == i, [x_axis_col_name, y_axis_col_name]]
            data_set_lists[i] = filtered_df  # Store filtered data
        plt.figure(figsize=(16, 8))
        for key, data in data_set_lists.items():
            plt.plot(data[x_axis_col_name], data[y_axis_col_name], label=key, marker='o')  # Added dots    
            for x, y in zip(data[x_axis_col_name], data[y_axis_col_name]):
                    plt.text(x, y, str(y), fontsize=12, ha="center", va="bottom")

        # Customize plot
        plt.xlabel(x_axis_col_name)
        plt.ylabel(y_axis_col_name)
        # plt.title(f"{line_graph_name} detail graph")
        plt.legend()  # Add legend to differentiate lines
        plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
        plt.grid(True)  # Add grid

        image_path = image_main_path+f"{line_graph_name}.png"
        plt.savefig(image_path, bbox_inches="tight")  # Save with tight layout
        plt.close()
        # Show the plot
        # plt.show()

        return image_path

    except Exception as e:
        print(f'---------line_graph_func (** error for {line_graph_name} **)-------------', e)
        # return str(e)

def bar_graph_function(bar_graph_name:str, col_name_uniq:str, x_axis_col_name:str, y_axis_col_name:str, bar_graph_query:str):
    try:
        query_str_bar_graph = re.sub(r"\s*=\s*:\s*daterange", f"{daterange_replace_str_more_then_one_day}", bar_graph_query)
        # print(f'***************************** before exceute (query_{bar_graph_name}) ******************************************')
        # print(query_str_bar_graph)
        # print(f'***************************** before exceute (query_{bar_graph_name}) ******************************************')
        data_set, column_names = snowflake_data_generate_html(query_str_bar_graph, f"{bar_graph_name}")
        df = pd.DataFrame(data_set, columns=column_names)
    
        # csv_path = csv_main_path+f"{bar_graph_name}.csv"
        # df.to_csv(csv_path)
        # print('=========', df)
        # # Read CSV
        # df = pd.read_csv(csv_path)

        
        df = df.sort_values(by=x_axis_col_name)
        each_data_date = df[x_axis_col_name].unique()
        each_data = df.sort_values(by=col_name_uniq, ascending=False)[col_name_uniq].unique()

        # Dictionary to store the datasets
        data_set_lists = {}
        for i in each_data:
            filtered_df = df.loc[df[col_name_uniq] == i, [x_axis_col_name, y_axis_col_name]]
            data_set_lists[i] = filtered_df  # Store filtered data
            
        p=np.arange(len(data_set_lists[next(iter(data_set_lists))]))
        width=0.2
        p1=[j+width for j in p]
        new_list = [p, p1]
        while len(new_list) != len(p1):
            new_list.append([round(j+width, 1) for j in new_list[-1]])

        # print('----', new_list)
        # print(data_set_lists)
        plt.figure(figsize=(16, 8))
        for index, (key, data) in enumerate(data_set_lists.items()):
            plt.bar(new_list[index], data[y_axis_col_name], width=width, label=key)  # Added dots    
            for x, y in zip(new_list[index], data[y_axis_col_name]):
                    plt.text(x, y, str(y), fontsize=12, ha="center", va="bottom")

        # Customize plot
        plt.xlabel(x_axis_col_name)
        plt.ylabel(y_axis_col_name)
        # plt.title(f"{bar_graph_name} detail graph")
        plt.legend()  # Add legend to differentiate lines
        plt.xticks(p+width, each_data_date, rotation=45)  # Rotate x-axis labels for better readability
        plt.grid(True)  # Add grid


        image_path = image_main_path+f"{bar_graph_name}.png"
        plt.savefig(image_path, bbox_inches="tight")  # Save with tight layout
        plt.close()
        # # # Show the plot
        # # # plt.show()

        return image_path

    except Exception as e:
        print(f'---------bar_graph_func (** error for {bar_graph_name} **)-------------', e)
        return str(e)

def daily_dashboard_after_chart_table(): 

    obj_table_query = dbb_tq()
    query_manufacturer_mapping = re.sub(r"\s*=\s*:\s*daterange", f"{daterange_replace_str_one_day}", obj_table_query.query_string_manufacturer_mapping)
    # print('***************************** before exceute (query_manufacturer_mapping) ******************************************')
    # print(query_manufacturer_mapping)
    # print('***************************** before exceute (query_manufacturer_mapping) ******************************************')
    table_html_manufacturer_mapping = snowflake_data_generate_html(query_manufacturer_mapping, "<h4>Manufacturer mapping stats</h4>", for_table=True)[0]
    
    # ------------------------------------------------------- Daily Dashboard End ----------------------------------------------- #

    query_overall_project_wise_automapping = re.sub(r"\s*=\s*:\s*daterange", f"{daterange_replace_str_more_then_one_day}", obj_table_query.query_string_overall_project_wise_automapping)
    # print('***************************** before exceute (query_overall_project_wise_automapping) ******************************************')
    # print(query_overall_project_wise_automapping)
    # print('***************************** before exceute (query_overall_project_wise_automapping) ******************************************')
    table_html_overall_project_wise_automapping = snowflake_data_generate_html(query_overall_project_wise_automapping, "<h4>Overall project wise mapping stats</h4>", for_table=True)[0]

    # ------------------------------------------------------- Manual Mapping End ----------------------------------------------- #

    set_of_html_table = table_html_manufacturer_mapping + table_html_overall_project_wise_automapping

    return set_of_html_table


def main():
    # Combine both reports and send the email

    # ------------------------------------- for tables -------------------------------------
    # table_html_1, table_html_2 = bulk_update_query()
    table_html_3 = daily_dashboard_before_chart_table()
    # combined_html_before_chart = table_html_1 + table_html_2 + table_html_3
    # ------------------------------------- for tables -------------------------------------


    # ------------------------------------- for line plot charts -------------------------------------
    # path_of_weekly_manual_efforts = line_graph_from_df_weekly_manual_efforts()
    # path_of_product_addition = line_graph_from_df_product_addition()
    obj_of_dbb_cq = dbb_cq()
    name_list_of_table_and_charts = ["Project wise daily stats", "Manual mapping stats", "Yesterday's Active User ", 
                                    "Average Active User", "Star performer for manual efforts", 
                                    "Manufacturer mapping stats", "Overall project wise mapping stats"]
    name_list_of_charts = ["Weekly manual efforts", "Weekly product addition ", "Pharmretail ", "Pharmanalytics", "Pharmconnect",]
    path_of_weekly_manual_efforts = line_graph_function(line_graph_name="weekly_manual_efforts", 
                                                        col_name_uniq="SOURCE_SYSTEM", 
                                                        x_axis_col_name="RECORD_DATE", 
                                                        y_axis_col_name="TOTAL MANUAL EFFORTS", 
                                                        line_graph_query=obj_of_dbb_cq.query_string_weekly_manual_efforts)
    
    path_of_product_addition = line_graph_function(line_graph_name="product_addition", 
                                                   col_name_uniq="SOURCETYPE1", 
                                                   x_axis_col_name="RECORD_DATE", 
                                                   y_axis_col_name="PRODUCT ADDITION", 
                                                   line_graph_query=obj_of_dbb_cq.query_string_product_addition)

    images_path_list_line_charts = [path_of_weekly_manual_efforts, path_of_product_addition]
    # ------------------------------------- for line plot charts -------------------------------------


    # ------------------------------------- for bar plot charts -------------------------------------
    path_of_pharmretail = bar_graph_function(bar_graph_name="pharmretail",  
                                               col_name_uniq="PARAMETER", 
                                               x_axis_col_name="RECORD_DATE",
                                               y_axis_col_name="RECORDS", 
                                               bar_graph_query=obj_of_dbb_cq.query_string_pharmretail)
                                               
    path_of_pharmanalytics = bar_graph_function(bar_graph_name="pharmanalytics",  
                                               col_name_uniq="PARAMETER", 
                                               x_axis_col_name="RECORD_DATE",
                                               y_axis_col_name="RECORDS", 
                                               bar_graph_query=obj_of_dbb_cq.query_string_pharmanalytics)
                                               
    path_of_pharmconnect = bar_graph_function(bar_graph_name="pharmconnect",  
                                               col_name_uniq="PARAMETER", 
                                               x_axis_col_name="RECORD_DATE",
                                               y_axis_col_name="RECORDS", 
                                               bar_graph_query=obj_of_dbb_cq.query_string_pharmconnect)
                                               
    images_path_list_bar_charts = [path_of_pharmretail, path_of_pharmanalytics, path_of_pharmconnect]
    # ------------------------------------- for bar plot charts -------------------------------------

    images_path_list = images_path_list_line_charts+images_path_list_bar_charts
    body_for_images = ""
    for img_no, name_of_chart in zip(range(len(images_path_list)), name_list_of_charts):
        body_for_images += f"<html><body><h4>{name_of_chart}:</h4><img src='cid:image{img_no}'></body></html>"
        
    # print('----', images_path_list)

    combined_html_after_chart = daily_dashboard_after_chart_table()

    send_email(
        "Report on MDM mapping tracker", 
        body = table_html_3 + body_for_images + combined_html_after_chart,
        image_path_list=images_path_list
    )

if __name__ == "__main__":
    main()