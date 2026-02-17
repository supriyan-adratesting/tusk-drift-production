from src import app
from src.controllers.admin import admin_process as admin
from src.controllers.jwt_tokens import jwt_token_required as jwt_token


@app.route('/get_company_names',endpoint='get_company_names',methods=['POST'])
@jwt_token.token_required
def professional_profile_update():
    result = admin.get_company_names()
    return result

@app.route('/get_sub_users',endpoint='get_sub_users',methods=['POST'])
@jwt_token.token_required
def get_sub_users():
    result = admin.get_sub_users()
    return result

@app.route('/get_job_list',endpoint='get_job_list',methods=['POST'])
@jwt_token.token_required
def get_job_list():
    result = admin.get_job_list()
    return result

@app.route('/get_professional_list',endpoint='get_professional_list',methods=['POST'])
@jwt_token.token_required
def get_professional_list():
    result = admin.get_professional_list()
    return result

@app.route('/send_expert_notes',endpoint='send_expert_notes',methods=['POST'])
@jwt_token.token_required
def send_expert_notes():
    result = admin.send_expert_notes()
    return result

@app.route('/get_professional_details',endpoint='get_professional_details',methods=['POST'])
@jwt_token.token_required
def get_professional_details():
    result = admin.get_professional_details()
    return result

@app.route('/make_recommendation',endpoint='make_recommendation',methods=['POST'])
@jwt_token.token_required
def make_recommendation():
    result = admin.make_recommendation()
    return result

@app.route('/delete_recommendation',endpoint='delete_recommendation',methods=['POST'])
@jwt_token.token_required
def delete_recommendation():
    result = admin.delete_recommendation()
    return result

@app.route('/individual_professional_detail',endpoint='individual_professional_detail',methods=['POST'])
@jwt_token.token_required
def individual_professional_detail():
    result = admin.individual_professional_detail()
    return result

@app.route('/admin_professional_dashboard',endpoint='admin_professional_dashboard',methods=['POST'])
@jwt_token.token_required
def admin_professional_dashboard():
    result = admin.admin_professional_dashboard()
    return result

@app.route('/individual_job_detail',endpoint='individual_job_detail',methods=['POST'])
@jwt_token.token_required
def individual_job_detail():
    result = admin.individual_job_details()
    return result

@app.route('/admin_jobs_dashboard',endpoint='admin_jobs_dashboard',methods=['POST'])
@jwt_token.token_required
def admin_jobs_dashboard():
    result = admin.admin_jobs_dashboard()
    return result

@app.route('/admin_employer_dashboard',endpoint='admin_employer_dashboard',methods=['POST'])
@jwt_token.token_required
def admin_employer_dashboard():
    result = admin.admin_employer_dashboard()
    return result

@app.route('/individual_employer_detail',endpoint='individual_employer_detail',methods=['POST'])
@jwt_token.token_required
def individual_employer_detail():
    result = admin.individual_employer_detail()
    return result

@app.route('/admin_partner_dashboard',endpoint='admin_partner_dashboard',methods=['POST'])
@jwt_token.token_required
def admin_partner_dashboard():
    result = admin.admin_partner_dashboard()
    return result

@app.route('/individual_partner_detail',endpoint='individual_partner_detail',methods=['POST'])
@jwt_token.token_required
def individual_partner_detail():
    result = admin.individual_partner_detail()
    return result

@app.route('/admin_custom_notes',endpoint='admin_custom_notes',methods=['POST'])
@jwt_token.token_required
def admin_custom_notes():
    result = admin.admin_custom_notes()
    return result

@app.route('/admin_professional_filter_results',endpoint='admin_professional_filter_results',methods=['POST'])
@jwt_token.token_required
def admin_professional_filter_results():
    result = admin.admin_professional_filter_results()
    return result

@app.route('/admin_job_filter_results',endpoint='admin_job_filter_results',methods=['POST'])
@jwt_token.token_required
def admin_job_filter_results():
    result = admin.admin_job_filter_results()
    return result

@app.route('/admin_employer_filter_results',endpoint='admin_employer_filter_results',methods=['POST'])
@jwt_token.token_required
def admin_employer_filter_results():
    result = admin.admin_employer_filter_results()
    return result

@app.route('/admin_partner_filter_results',endpoint='admin_partner_filter_results',methods=['POST'])
@jwt_token.token_required
def admin_partner_filter_results():
    result = admin.admin_partner_filter_results()
    return result

# @app.route('/admin_professional_dashboard_search',endpoint='admin_professional_dashboard_search',methods=['POST'])
# @jwt_token.token_required
# def admin_professional_dashboard_search():
#     result = admin.admin_professional_dashboard_search()
#     return result

@app.route('/admin_employer_dashboard_search',endpoint='admin_employer_dashboard_search',methods=['POST'])
@jwt_token.token_required
def admin_employer_dashboard_search():
    result = admin.admin_employer_dashboard_search()
    return result

@app.route('/admin_partner_dashboard_search',endpoint='admin_partner_dashboard_search',methods=['POST'])
@jwt_token.token_required
def admin_partner_dashboard_search():
    result = admin.admin_partner_dashboard_search()
    return result

@app.route('/admin_professional_meilisearch',endpoint='admin_professional_meilisearch',methods=['POST'])
@jwt_token.token_required
def admin_professional_meilisearch():
    result = admin.admin_professional_meilisearch_filter_results()
    return result

# @app.route('/admin_professional_meilisearch_filter_results_new',endpoint='admin_professional_meilisearch_filter_results_new',methods=['POST'])
# @jwt_token.token_required
# def admin_professional_meilisearch_filter_results_new():
#     result = admin.admin_professional_meilisearch_filter_results_new()
#     return result

@app.route('/admin_employer_meilisearch',endpoint='admin_employer_meilisearch',methods=['POST'])
@jwt_token.token_required
def admin_employer_meilisearch():
    result = admin.admin_employer_meilisearch_filter_results()
    return result

@app.route('/admin_partner_meilisearch',endpoint='admin_partner_meilisearch',methods=['POST'])
@jwt_token.token_required
def admin_partner_meilisearch():
    result = admin.admin_partner_meilisearch_filter_results()
    return result

@app.route('/admin_jobs_meilisearch',endpoint='admin_jobs_meilisearch',methods=['POST'])
@jwt_token.token_required
def admin_jobs_meilisearch():
    result = admin.admin_jobs_meilisearch_filter_results()
    return result

@app.route('/admin_best_fit_applicants',endpoint='admin_best_fit_applicants',methods=['POST'])
@jwt_token.token_required
def admin_best_fit_applicants():
    result = admin.super_admin_best_fit_applicants()
    return result

@app.route('/get_job_share_link',endpoint='get_job_share_link',methods=['POST'])
def get_job_share_link():
    result = admin.get_job_share_link()
    return result

@app.route('/get_users_feedback',endpoint='get_users_feedback',methods=['POST'])
@jwt_token.token_required
def get_users_feedback():
    result = admin.get_users_feedback()
    return result

@app.route('/get_user_summary',endpoint='get_user_summary',methods=['POST'])
@jwt_token.token_required
def get_user_summary():
    result = admin.get_user_summary()
    return result

@app.route('/feedback_search',endpoint='feedback_search',methods=['POST'])
@jwt_token.token_required
def feedback_search():
    result = admin.feedback_search()
    return result

@app.route('/admin_make_recommendation_search',endpoint='admin_make_recommendation_search',methods=['POST'])
@jwt_token.token_required
def admin_make_recommendation_search():
    result = admin.admin_make_recommendation_search()
    return result

@app.route('/admin_professional_dashboard_search_filter',endpoint='admin_professional_dashboard_search_filter',methods=['POST'])
@jwt_token.token_required
def admin_professional_dashboard_search_filter():
    result = admin.admin_professional_dashboard_search_filter()
    return result

@app.route('/website_data',endpoint='website_data',methods=['GET'])
def website_data():
    result = admin.website_data()
    return result

@app.route('/sso',endpoint='sso',methods=['GET'])
# @jwt_token.token_required
def discourse_sso():
    result = admin.sso()
    return result

@app.route('/discourse_webhook',endpoint='discourse_webhook',methods=['POST'])
# @jwt_token.token_required
def discourse_webhook():
    result = admin.discourse_webhook()
    return result

@app.route('/admin_upload_event',endpoint='admin_upload_event',methods=['POST'])
@jwt_token.token_required
def admin_upload_event():
    result = admin.admin_upload_event()
    return result

@app.route('/admin_update_event',endpoint='admin_update_event',methods=['POST'])
@jwt_token.token_required
def admin_update_event():
    result = admin.admin_update_event()
    return result

@app.route('/admin_delete_event',endpoint='admin_delete_event',methods=['POST'])
@jwt_token.token_required
def admin_delete_event():
    result = admin.admin_delete_event()
    return result


@app.route("/upload_job_excel",endpoint='upload_job_excel' , methods=["POST"])
@jwt_token.token_required
def upload_job_excel():
    result = admin.upload_job_excel()
    return result

@app.route('/get_admin_jobs_post',endpoint='get_admin_jobs_post',methods=['POST'])
@jwt_token.token_required
def get_admin_jobs_post():
    result = admin.get_admin_jobs_post()
    return result

@app.route('/get_individual_external_job_details',endpoint='get_individual_external_job_details',methods=['POST'])
@jwt_token.token_required
def get_individual_external_job_details():
    result = admin.get_individual_external_job_details()
    return result


@app.route('/create_training_event',endpoint='create_training_event',methods=['POST'])
@jwt_token.token_required
def create_training_event():
    result = admin.create_training_event()
    return result


@app.route('/admin_training_update_event',endpoint='admin_training_update_event',methods=['POST'])
@jwt_token.token_required
def admin_training_update_event():
    result = admin.admin_training_update_event()
    return result

@app.route('/admin_training_delete_event',endpoint='admin_training_delete_event',methods=['POST'])
@jwt_token.token_required
def admin_training_delete_event():
    result = admin.admin_training_delete_event()
    return result


@app.route('/job_preview_details',endpoint='job_preview_details',methods=['GET'])
def job_preview_details():
    result = admin.job_preview_details()
    return result

@app.route('/employer_assist_job_count',endpoint='employer_assist_job_count',methods=['POST'])
@jwt_token.token_required
def employer_assist_job_count():
    result = admin.employer_assist_job_count()
    return result

@app.route('/assist_job_decrease_count',endpoint='assist_job_decrease_count',methods=['POST'])
@jwt_token.token_required
def assist_job_decrease_count():
    result = admin.assist_job_decrease_count()
    return result