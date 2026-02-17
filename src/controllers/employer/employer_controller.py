from flask import render_template
from flask import Flask, request, jsonify, session, url_for, redirect

from src import app
from src.controllers.professional import professional_process as professional
from src.controllers.employer import employer_process as employer
from src.controllers.jwt_tokens import jwt_token_required as jwt_token


@app.route('/professional_signup' ,methods=['GET'])
def recruter_signup():
    result = professional.recruter_signup()
    # if 'ID' in request.headers:
    #     user_id = request.headers['ID'] 
    #     result = professional.professional_signup()
    # else:
    #     return (jsonify({"message": "ID required"}), 401) 
    return result

@app.route('/employer_job_post' ,methods=['POST'])
@jwt_token.token_required
def employer_job_post():
    result = employer.employer_job_post()
    return result

@app.route('/get_job_post',endpoint='get_job_post',methods=['POST'])
@jwt_token.token_required
def get_job_post():
    result = employer.get_job_posts()
    return result

@app.route('/employer_profile_dashboard',endpoint='employer_profile_dashboard' ,methods=['GET'])
@jwt_token.token_required
def employer_profile_dashboard():
    return employer.get_employer_profile_dashboard_data()

@app.route('/get_org_description',endpoint='get_org_description',methods=['GET'])
@jwt_token.token_required
def get_org_description():
    result = employer.get_org_description()
    return result

@app.route('/update_org_description',endpoint='update_org_description',methods=['POST'])
@jwt_token.token_required
def update_org_description():
    result = employer.update_org_description()
    return result

@app.route('/get_hiring_team_details',endpoint='get_hiring_team_details',methods=['GET'])
@jwt_token.token_required
def get_hiring_team_details():
    result = employer.get_hiring_team_details()
    return result

@app.route('/update_hiring_team_details',endpoint='update_hiring_team_details',methods=['POST'])
@jwt_token.token_required
def update_hiring_team_details():
    result = employer.update_hiring_team_details()
    return result

@app.route('/assign_jobs',endpoint='assign_jobs',methods=['POST'])
@jwt_token.token_required
def assign_jobs():
    result = employer.assign_jobs()
    return result

@app.route('/add_team_members',endpoint='add_team_members',methods=['POST'])
@jwt_token.token_required
def add_team_members():
    result = employer.add_team_members()
    return result

@app.route('/update_team_members',endpoint='update_team_members',methods=['POST'])
@jwt_token.token_required
def update_team_members():
    result = employer.update_team_members()
    return result

@app.route('/delete_team_members',endpoint='delete_team_members',methods=['POST'])
@jwt_token.token_required
def delete_team_members():
    result = employer.delete_team_members()
    return result

@app.route('/delete_hiring_team_details',endpoint='delete_hiring_team_details',methods=['POST'])
@jwt_token.token_required
def delete_hiring_team_details():
    result = employer.delete_hiring_team_details()
    return result

@app.route('/get_company_details',endpoint='get_company_details',methods=['GET'])
@jwt_token.token_required
def get_company_details():
    result = employer.get_company_details()
    return result

@app.route('/get_home_dashboard',endpoint='get_home_dashboard',methods=['POST'])
@jwt_token.token_required
def get_home_dashboard_view():
    result = employer.home_dashboard_view()
    return result

@app.route('/get_new_home_dashboard',endpoint='get_new_home_dashboard',methods=['POST'])
@jwt_token.token_required
def get_new_home_dashboard_view():
    result = employer.new_home_dashboard_view()
    return result

@app.route('/close_job',endpoint='close_job',methods=['POST'])
@jwt_token.token_required
def update_close_job_posted():
    result = employer.close_job_posted()
    return result

@app.route('/update_status_job_post',endpoint='update_status_job_post',methods=['POST'])
@jwt_token.token_required
def status_update_job_posted():
    result = employer.status_update_job_posted()
    return result

@app.route('/update_interview_status',endpoint='update_interview_status',methods=['POST'])
@jwt_token.token_required
def update_interview_status():
    result = employer.update_interview_status()
    return result

@app.route('/get_applied_close_job',endpoint='get_applied_close_job',methods=['POST'])
@jwt_token.token_required
def get_applied_close_job_view():
    result = employer.get_applied_close_job_data()
    return result

@app.route('/delete_job_post',endpoint='delete_job_post',methods=['POST'])
@jwt_token.token_required
def delete_job_post():
    result = employer.delete_job_post()
    return result

@app.route('/update_company_details',endpoint='update_company_details',methods=['POST'])
@jwt_token.token_required
def update_company_details():
    result = employer.update_company_details()
    return result

@app.route('/on_click_load_more',endpoint='on_click_load_more',methods=['POST'])
@jwt_token.token_required
def on_click_load_more():
    result = employer.on_click_load_more()
    return result

@app.route('/pool_dashboard_view',endpoint='pool_dashboard_view',methods=['POST'])
@jwt_token.token_required
def pool_dashboard_view():
    result = employer.pool_dashboard_view()
    return result

@app.route('/candidates_dashboard_view',endpoint='candidates_dashboard_view',methods=['POST'])
@jwt_token.token_required
def candidates_dashboard_view():
    result = employer.candidates_dashboard_view()
    return result

@app.route('/applicants_view_mail',endpoint='applicants_view_mail',methods=['POST'])
@jwt_token.token_required
def applicants_view_mail():
    result = employer.applicants_view_mail()
    return result

@app.route('/get_selected_professional_detail',endpoint='get_selected_professional_detail',methods=['POST'])
@jwt_token.token_required
def get_selected_professional_detail():
    result = employer.get_selected_professional_detail()
    return result

@app.route('/update_application_status',endpoint='update_application_status',methods=['POST'])
@jwt_token.token_required
def update_application_status():
    result = employer.update_application_status()
    return result

@app.route('/update_custom_notes',endpoint='update_custom_notes',methods=['POST'])
@jwt_token.token_required
def update_custom_notes():
    result = employer.update_custom_notes()
    return result

@app.route('/filter_by_application_status',endpoint='filter_by_application_status',methods=['POST'])
@jwt_token.token_required
def filter_by_application_status():
    result = employer.filter_by_application_status()
    return result

@app.route('/filter_professionals',endpoint='filter_professionals',methods=['POST'])
@jwt_token.token_required
def filter_professionals():
    result = employer.filter_professionals()
    return result

@app.route('/draft_job_post',endpoint='draft_job_post',methods=['POST'])
@jwt_token.token_required
def draft_job_post():
    result = employer.job_post_draft()
    return result

@app.route('/get_other_professional_skills',endpoint='get_other_professional_skills',methods=['GET'])
@jwt_token.token_required
def get_other_professional_skills():
    result = employer.get_other_professional_skills()
    return result

@app.route('/get_payment_status_employer',endpoint='get_payment_status_employer' ,methods=['GET'])
@jwt_token.token_required
def get_payment_status_employer():
    return employer.get_payment_status_employer()

@app.route('/edit_employer_job_post',endpoint='edit_employer_job_post' ,methods=['POST'])
@jwt_token.token_required
def edit_employer_job_post():
    return employer.edit_employer_job_post()

@app.route('/pool_dashboard_search',endpoint='pool_dashboard_search',methods=['POST'])
@jwt_token.token_required
def pool_dashboard_search():
    result = employer.pool_dashboard_search()
    return result

@app.route('/pool_dashboard_meilisearch',endpoint='pool_dashboard_meilisearch',methods=['POST'])
@jwt_token.token_required
def pool_dashboard_meilisearch():
    result = employer.pool_dashboard_meilisearch()
    return result

@app.route('/pool_dashboard_search_filter',endpoint='pool_dashboard_search_filter',methods=['POST'])
@jwt_token.token_required
def pool_dashboard_search_filter():
    result = employer.pool_dashboard_search_filter()
    return result
  
@app.route('/fetch_employer_jobs',endpoint='fetch_employer_jobs',methods=['POST'])
@jwt_token.token_required
def fetch_employer_jobs():
    result = employer.fetch_employer_jobs()
    return result

@app.route('/invite_by_employer',endpoint='invite_by_employer',methods=['POST'])
@jwt_token.token_required
def invite_by_employer():
    result = employer.invite_by_employer()
    return result

@app.route('/invited_applicants_view',endpoint='invited_applicants_view',methods=['POST'])
@jwt_token.token_required
def invited_applicants_view():
    result = employer.invited_applicants_view()
    return result

@app.route('/invited_applicants_search',endpoint='invited_applicants_search',methods=['POST'])
@jwt_token.token_required
def invited_applicants_search():
    result = employer.invited_applicants_search()
    return result

@app.route('/best_fit_applicants',endpoint='best_fit_applicants',methods=['POST'])
@jwt_token.token_required
def best_fit_applicants():
    result = employer.best_fit_applicants()
    return result


# @app.route('/store_customer_gst',endpoint='store_customer_gst' ,methods=['POST'])
# @jwt_token.token_required
# def store_customer_gst():
#     result = employer.store_customer_gst()
#     return result

# @app.route('/get_stored_customer_gst_details',endpoint='get_stored_customer_gst_details' ,methods=['GET'])
# @jwt_token.token_required
# def get_stored_customer_gst_details():
#     result = employer.get_stored_customer_gst_details()
#     return result